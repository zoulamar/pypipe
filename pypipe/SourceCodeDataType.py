from . import GenericDataType
import numpy as np
from typing import Dict

class SourceCodeDataType(GenericDataType):
    """ A Data Type which stores ground truth data together with predicted values and offers methods for evaluation. """

    def load(self) -> Dict[str, np.ndarray]:
        raise FileExistsError("Source files are not intended for opening.")

    def save(self):
        raise FileExistsError("Source files are not intended for opening.")

    def str_detailed(self):
        print(f"Source code saved as file {self.path}")
