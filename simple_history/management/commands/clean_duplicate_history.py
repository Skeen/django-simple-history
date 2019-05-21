from django.utils import timezone
from django.db import transaction

from . import populate_history
from ... import models, utils
from ...exceptions import NotHistoricalModelError


class Command(populate_history.Command):
    args = "<app.model app.model ...>"
    help = (
        "Scans HistoricalRecords for identical sequencial entries "
        "(duplicates) in a model and deletes them."
    )

    DONE_CLEANING_FOR_MODEL = "Removed {count} historical records for {model}\n"

    def add_arguments(self, parser):
        parser.add_argument("models", nargs="*", type=str)
        parser.add_argument(
            "--auto",
            action="store_true",
            dest="auto",
            default=False,
            help="Automatically search for models with the HistoricalRecords field "
            "type",
        )
        parser.add_argument(
            "-d", "--dry", action="store_true", help="Dry (test) run only, no changes"
        )
        parser.add_argument(
            "-m", "--minutes", type=int, help="Only search the last MINUTES of history"
        )

    def handle(self, *args, **options):
        self.verbosity = options["verbosity"]

        to_process = set()
        model_strings = options.get("models", []) or args

        if model_strings:
            for model_pair in self._handle_model_list(*model_strings):
                to_process.add(model_pair)

        elif options["auto"]:
            to_process = self._auto_models()

        else:
            self.log(self.COMMAND_HINT)

        self._process(to_process, date_back=options["minutes"], dry_run=options["dry"])

    def _process(self, to_process, date_back=None, dry_run=True):
        if date_back:
            stop_date = timezone.now() - timezone.timedelta(minutes=date_back)
        else:
            stop_date = None

        for model, history_model in to_process:
            m_qs = history_model.objects
            if stop_date:
                m_qs = m_qs.filter(history_date__gte=stop_date)
            found = m_qs.count()
            self.log("{0} has {1} historical entries".format(model, found), 2)
            if not found:
                continue

            from django.db.models import Max
            import math
            max_id = m_qs.aggregate(Max('id'))['id__max']

            history_fields = [
                'id', 'history_id', 'history_date', 'history_change_reason', 'history_type', 'history_user',
            ]
            table_name = history_model._meta.db_table
            data_fields = [
                f.name for f in history_model._meta.get_fields()
                if f.name not in history_fields
            ]
            query = """
            SELECT history_id FROM (
                SELECT
                    history_id, id,
            """
            query += ",".join(["""
                {0} as field_{1},
                LEAD({0}) OVER(PARTITION BY id ORDER BY history_date DESC) as history_{1}
            """.format(value, idx) for (idx, value) in enumerate(data_fields)
            ])
            query += """
                FROM
                    {0}
                WHERE 
                    id >= %s AND id < %s
                ) AS sub_table
            WHERE
            """.format(table_name)
            query += " AND ".join(["""
                field_{0} = history_{0} OR (field_{0} is NULL AND history_{0} is NULL)
            """.format(idx) for (idx, value) in enumerate(data_fields)
            ])

            from django.db.models import Window
            from django.db.models.functions import Lead
            from django.db.models import F, Q

            # Delete history in blocks, to avoid locking issues
            step_size = 10**5
            max_iterations = int(math.ceil(max_id / step_size))
            entries_deleted = 0
            for x in range(0, max_iterations + 1):
                with transaction.atomic(savepoint=True):
                    listy = [obj.pk for obj in m_qs.raw(query, [x * step_size, (x + 1) * step_size])]
                    entries_deleted += len(listy)
                    if not dry_run:
                        m_qs.filter(pk__in=listy).delete()

            self.log(
                self.DONE_CLEANING_FOR_MODEL.format(model=model, count=entries_deleted)
            )


                    
#                    # Create blocks
#                    q = m_qs.filter(
#                        id__gt=x * step_size,
#                        id__lt=(x + 1) * step_size
#                    )
#                    # Windowing to see previous record via LEAD
#                    window = {
#                        'partition_by': [F('id')],
#                        'order_by': F('history_date').desc(),
#                    }
#                    q = q.annotate(
#                        # Alias fields for uniform access
#                        **{'field_' + str(idx): F(value) for (idx, value) in enumerate(data_fields)}
#                    ).annotate(
#                        # Find history fields
#                        **{'history_' + str(idx): Window(expression=Lead(value), **window)
#                            for (idx, value) in enumerate(data_fields)
#                        }
#                    )
#
#                    from django.db.models import Case, BooleanField, Value, When
#                    q = q.annotate(
#                        **{'field_ok_' + str(idx): Case(
#                            When(**{'field_' + str(idx): F('history_' + str(idx))}, then=Value(True)),
#                            default=Value(True),
#                            output_field=BooleanField()
#                        ) for (idx, _) in enumerate(data_fields)}
#                    )
#                    print(q.query)
#                    # This yields: sqlite3.OperationalError: misuse of window function LEAD()
#                    q = q.filter(
#                        **{'field_' + str(idx): F('history_' + str(idx)) 
#                            for (idx, value) in enumerate(data_fields)
#                        }
#                    )
#
#                    from django.db.models import Value, IntegerField
#                    # NOTE: Hack to avoid: django.db.utils.NotSupportedError: Window is disallowed in the filter clause.
#                    # NOTE: See below
#                    q = q.annotate(
#                        always_null=Value(None, output_field=IntegerField())
#                    )
#                    # The hack yields: sqlite3.OperationalError: misuse of window function LEAD()
#                    for (idx, _) in enumerate(data_fields):
#                        q = q.filter(
#                            Q(**{'field_' + str(idx): F('history_' + str(idx))}) | Q(Q(always_null=F('field_' + str(idx))), Q(always_null=F('history_' + str(idx))))
#                        )
#
#                    # NOTE: django.db.utils.NotSupportedError: Window is disallowed in the filter clause.
#                    q = q.filter(
#                        *[
#                            Q(
#                                Q(**{'field_' + str(idx): F('history_' + str(idx))}) |
#                                Q(**{
#                                    'field_' + str(idx) + '__isnull': True,
#                                    'history_' + str(idx) + '__isnull': True,
#                                })
#                            )
#                            for (idx, value) in enumerate(data_fields)
#                        ]
#                    )
#
#                    m_qs.filter(pk__in=q.values_list('pk', flat=True)).delete()

            # it would be great if we could just iterate over the instances that
            # have changes (in the given period) but
            # `m_qs.values(model._meta.pk.name).distinct()`
            # is actually slower than looping all and filtering in the code...
#            for o in model.objects.all():
#                self._process_instance(o, model, stop_date=stop_date, dry_run=dry_run)

    def _process_instance(self, instance, model, stop_date=None, dry_run=True):
        entries_deleted = 0
        history = utils.get_history_manager_for_model(instance)
        o_qs = history.all()
        if stop_date:
            # to compare last history match
            extra_one = o_qs.filter(history_date__lte=stop_date).first()
            o_qs = o_qs.filter(history_date__gte=stop_date)
        else:
            extra_one = None
        with transaction.atomic():
            # ordering is ('-history_date', '-history_id') so this is ok
            f1 = o_qs.first()
            if not f1:
                return

            for f2 in o_qs[1:]:
                entries_deleted += self._check_and_delete(f1, f2, dry_run)
                f1 = f2
            if extra_one:
                entries_deleted += self._check_and_delete(f1, extra_one, dry_run)

        self.log(
            self.DONE_CLEANING_FOR_MODEL.format(model=model, count=entries_deleted)
        )

    def log(self, message, verbosity_level=1):
        if self.verbosity >= verbosity_level:
            self.stdout.write(message)

    def _check_and_delete(self, entry1, entry2, dry_run=True):
        delta = entry1.diff_against(entry2)
        if not delta.changed_fields:
            if not dry_run:
                entry1.delete()
            return 1
        return 0
