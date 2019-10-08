from celery import Celery, Task

from glide.extensions.celery import *

app = Celery("app", broker="redis://localhost", backend="redis://localhost")

app.conf.update(
    result_expires=3600,
    accept_content=["pickle", "json"],
    worker_hijack_root_logger=False,
)

# Celery tries to do a ping and relies on a ping task to be present
app.loader.import_module("celery.contrib.testing.tasks")

# -------- Basic task(s) for testing

# Repeating this here as Celery is finicky with imports
def lower_rows(data):
    for row in data:
        for k, v in row.items():
            if isinstance(v, str):
                row[k] = v.lower()
    return data


@app.task
def lower_task(data):
    return lower_rows(data)


# -------- Glide tasks/helpers

celery_consume_task = app.register_task(CeleryConsumeTask())
celery_glider_task = app.register_task(CeleryGliderTask())
celery_glider_template_task = app.register_task(CeleryGliderTemplateTask())
celery_build_glider_task = app.register_task(CeleryBuildGliderTask())
