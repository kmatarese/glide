try:
    import swifter
except ImportError:
    swifter = None

from glide.core import Node


class SwifterApplyTransformer(Node):
    """Apply a Swifter transform to a Pandas DataFrame"""

    def run(self, df, func, processes=True, **kwargs):
        """Use Swifter apply() on a DataFrame"""
        assert swifter, "The swifter package is not installed"
        if processes:
            df.swifter.set_dask_scheduler(scheduler="processes")
        else:
            df.swifter.set_dask_scheduler(scheduler="threads")
        for column in df.columns:
            df[column] = df[column].swifter.apply(func, **kwargs)
        self.push(df)
