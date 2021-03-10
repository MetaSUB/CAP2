"""Custom exceptions."""


class EmptyGroupResult(Exception):
    """Exception raised when group AnalysisModule is run with <= 1 samples."""

    pass


class UnsupportedAnalysisMode(NotImplementedError):
    """
    Error raised when AnalysisModule is called in the wrong context.
    Example: an AnalysisModule that processes all sample data for a SampleGroup
             gets called for processing a single Sample.
    """

    pass
