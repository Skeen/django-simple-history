from django.utils import timezone
from django.db import transaction
from django.db.models import Max

from . import populate_history
from ... import models, utils
from ...exceptions import NotHistoricalModelError
import math


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

        history_fields = [
            'id', 'history_id', 'history_date', 'history_change_reason',
            'history_type', 'history_user',
        ]
        for model, history_model in to_process:
            m_qs = history_model.objects
            if stop_date:
                m_qs = m_qs.filter(history_date__gte=stop_date)
            found = m_qs.count()
            self.log("{0} has {1} historical entries".format(model, found), 2)
            if not found:
                continue

            # TODO: Handle stop_date
            max_id = m_qs.aggregate(Max('id'))['id__max']

            table_name = history_model._meta.db_table
            data_fields = [
                f.name for f in history_model._meta.get_fields()
                if f.name not in history_fields
            ]
            query = """
            SELECT history_id FROM (
                SELECT
                    history_id,
                    id,
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

    def log(self, message, verbosity_level=1):
        if self.verbosity >= verbosity_level:
            self.stdout.write(message)
