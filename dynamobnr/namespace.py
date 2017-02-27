# -*- coding: utf-8 -*-


class Namespace(object):
    def __init__(self, adict = None):
        if adict is not None:
            self.__dict__.update(adict)

    def update(self, adict):
        self.__dict__.update(adict)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __str__(self):
        return str(self.__dict__)
