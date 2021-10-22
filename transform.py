from file_handler import *


class TransformJob:
    def __init__(self, file_handler: InOutBase):
        self._file_handler = file_handler

    def read(self, path):
        return self._file_handler.read(path)

    def write(self, path, data):
        self._file_handler.write(data, path)

    @staticmethod
    def transform(raw_data):
        """Finish me!"""
        transformed_data = None
        return transformed_data

    def execute(self, input_path, output_path):
        input_data = self.read(input_path)
        output_data = self.transform(input_data)
        self.write(output_path, output_data)


if __name__ == "__main__":
    input_path = "..."
    output_path = "output.csv"
    TransformJob(InOutBase()).execute(input_path=input_path, output_path=output_path)

