class Usage(object):
    # It needs to know how to get data, score usage
    def __init__(self, *conditions):
        # conditions are used to define how to prepare data
        # source, model.
        # Derived classes may have their own set up conditions:
        # usage model, prices(?), etc
        pass

    def prepare(self):
        # get data from source, do other preparations
        pass

    def calculate(self):
        print("Nothing to see here")
        return []


class NovaUsage(Usage):
    def calculate(self):
        print("%s called" % self.__class__.__name__)
        return []

