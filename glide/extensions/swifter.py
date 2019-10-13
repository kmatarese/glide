"""https://github.com/jmcarpenter2/swifter"""

try:
    import swifter
except ImportError:
    swifter = None

from glide.core import Node


class SwifterApply(Node):
    """Apply a Swifter transform to a Pandas DataFrame"""

    def run(self, df, func, processes=True, **kwargs):
        """Use Swifter apply() on a DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            The pandas DataFrame to apply func to
        func : callable
            A callable that will be passed to df.swifter.apply
        processes : bool
            If true use the "processes" scheduler, else "threads"
        **kwargs
            Keyword arguments passed to Dask df.swifter.apply

        """
        assert swifter, "The swifter package is not installed"
        if processes:
            df.swifter.set_dask_scheduler(scheduler="processes")
        else:
            df.swifter.set_dask_scheduler(scheduler="threads")
        for column in df.columns:
            df[column] = df[column].swifter.apply(func, **kwargs)
        self.push(df)
