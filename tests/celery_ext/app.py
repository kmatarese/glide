from celery import Celery

from glide.extensions.celery import *

app = Celery("app", broker="redis://localhost", backend="redis://localhost")

app.conf.update(result_expires=3600, accept_content=["pickle", "json"])

# Celery tries to do a ping and relies on a ping task to be present
app.loader.import_module("celery.contrib.testing.tasks")

# -------- Basic task(s) for testing


@app.task
def lower_task(data):
    for row in data:
        for k, v in row.items():
            if isinstance(v, str):
                row[k] = v.lower()
    return data


# -------- Glide tasks/helpers

celery_consume_task = app.register_task(CeleryConsumeTask())
celery_glider_task = app.register_task(CeleryGliderTask())
celery_glider_template_task = app.register_task(CeleryGliderTemplateTask())
celery_build_glider_task = app.register_task(CeleryBuildGliderTask())
