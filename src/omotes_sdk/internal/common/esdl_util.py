from esdl.esdl_handler import EnergySystemHandler


def pyesdl_from_string(input_str: str) -> EnergySystemHandler:
    """
    Loads esdl file from a string into memory.

    Please note that it is not checked if the contents of the string is a valid esdl.
    :param input_str: string containing the contents of an esdl file.
    """
    esh = EnergySystemHandler()
    esh.load_from_string(input_str)
    return esh
