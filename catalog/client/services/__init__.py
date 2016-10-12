def getKeysFromDict(dataDict, mapList):
    """
    Return value from a multiple key given by tuple representation e.g. (mainkey, subkey1, subkey2)
    :param dataDict: whole dictionary
    :param mapList: key name tuple
    :return: value of multiple key
    """
    return reduce(lambda my_dict, key: my_dict[key], mapList, dataDict)
