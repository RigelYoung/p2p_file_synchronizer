class ConversionUtils:
    #将Byte(B)转换为Megabyte(M)
    def bytes2Megabytes(bytesNum):
        return bytesNum / 1024 / 1024

    #将Megabyte(M)转换为Byte(B)
    def megabytes2Bytes(megaNum):
        return megaNum * 1024 * 1024