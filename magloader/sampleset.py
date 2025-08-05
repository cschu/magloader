import lxml.builder

from .sample import Sample


class SampleSet:
    def __init__(self):
        self.samples = []
    def toxml(self):
        maker = lxml.builder.ElementMaker()
        # sample_set = maker.SAMPLE_SET

        doc = maker.SAMPLE_SET(
            *(
                sample.toxml()
                for sample in self.samples
            )
        )

        return doc

    def get_base(self):
        return self.__class__

    @staticmethod
    def parse_submission_response(response):
        yield from Sample.parse_submission_response(response)
