# -*- coding: UTF-8 -*-


class PandasticSearchException(RuntimeError):
    def __init__(self, msg):
        super(PandasticSearchException, self).__init__(msg)


class NoSuchDependencyException(PandasticSearchException):
    pass


class ServerDefinedException(PandasticSearchException):
    pass


class ParseResultException(PandasticSearchException):
    pass


class DataFrameException(PandasticSearchException):
    pass
