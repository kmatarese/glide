"""https://github.com/jmcarpenter2/swifter"""

try:
    import swifter
except ImportError:
    swifter = None

from glide.core import Node
from glide.utils import raiseifnot


class SwifterApply(Node):
    """Apply a Swifter transform to a Pandas DataFrame"""

    def run(self, df, func, threads=False, **kwargs):
        """Use Swifter apply() on a DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            The pandas DataFrame to apply func to
        func : callable
            A callable that will be passed to df.swifter.apply
        threads : bool
            If true use the "threads" scheduler, else "processes"
        **kwargs
            Keyword arguments passed to Dask df.swifter.apply

        """
        raiseifnot(swifter, "The swifter package is not installed")

        if threads:
            df.swifter.set_dask_scheduler(scheduler="threads")
        else:
            df.swifter.set_dask_scheduler(scheduler="processes")

        for column in df.columns:
            df[column] = df[column].swifter.apply(func, **kwargs)
        self.push(df)
