from celery import Task
import numpy as np
from tlbx import st, set_missing_key, import_object

from glide import *
from glide.extensions.celery import app


# XXX is there a better way to org utility tasks?
@app.task
def lower(data):
    for row in data:
        for k, v in row.items():
            if isinstance(v, str):
                row[k] = v.lower()
    return data


class CeleryTaskDelay(Node):
    def run(self, data, task, timeout=None, push_type="async_result", **kwargs):
        async_result = task.delay(data, **kwargs)

        if push_type == "async_result":
            self.push(async_result)
        elif push_type == "input":
            self.push(data)
        elif push_type == "result":
            result = async_result.get(timeout=timeout)
            async_result.forget()
            self.push(result)
        else:
            assert False, "Invalid push_type: %s" % push_type

        # Celery group and/or chord/chain?
        # https://docs.celeryproject.org/en/latest/userguide/canvas.html#the-primitives


class CeleryPipelineTask(Task):
    def run(self, data, node_init, glider_kwargs=None, consume_kwargs=None):
        nodes = None

        for node_init_layer in node_init:
            if not isinstance(node_init_layer, (list, tuple)):
                node_init_layer = [node_init_layer]

            node_layer = []
            for node_info in node_init_layer:
                # TODO: this only supports a single level of node tree evaluation
                assert isinstance(node_info, dict), "Node info must be in dict format"
                node_args = node_info["args"]
                node_kwargs = node_info.get("kwargs", {})
                cls = import_object(node_info["class_name"])
                node = cls(*node_args, **node_kwargs)
                node_layer.append(node)

            if len(node_layer) == 1:
                node_layer = node_layer[0]

            if nodes is None:
                nodes = node_layer
                continue
            nodes = nodes | node_layer

        glider = Glider(nodes, **(glider_kwargs or {}))
        glider.consume(data, **(consume_kwargs or {}))


CeleryPipelineTask = app.register_task(CeleryPipelineTask())


@app.task(serializer="pickle")
def celery_consume(pipeline, split, cleanup=None, **node_contexts):
    consume(pipeline, split, cleanup=cleanup, **node_contexts)


class CeleryParaGlider(ParaGlider):
    """A ParaGlider that uses Celery to execute parallel calls to consume()"""

    def consume(self, data, cleanup=None, **node_contexts):
        """Setup node contexts and consume data with the pipeline

        Parameters
        ----------
        data
            Iterable of data to consume
        cleanup : dict, optional
            A mapping of arg names to clean up functions to be run after
            data processing is complete.
        **node_contexts
            Keyword arguments that are node_name->param_dict

        """

        # XXX

        # assert Celery, "Please install dask (Client) to use DaskParaGlider"
        worker_count = 4  # ???

        splits = np.array_split(data, min(len(data), worker_count))
        dbg(
            "%s: data len: %s, %d worker(s), %d split(s)"
            % (self.__class__.__name__, len(data), worker_count, len(splits))
        )
        for split in splits:
            celery_consume.delay(self.pipeline, split, cleanup=cleanup, **node_contexts)
