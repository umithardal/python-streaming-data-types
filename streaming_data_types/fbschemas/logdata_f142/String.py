# automatically generated by the FlatBuffers compiler, do not modify

# namespace:

import flatbuffers


class String(object):
    __slots__ = ["_tab"]

    @classmethod
    def GetRootAsString(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = String()
        x.Init(buf, n + offset)
        return x

    # String
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # String
    def Value(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None


def StringStart(builder):
    builder.StartObject(1)


def StringAddValue(builder, value):
    builder.PrependUOffsetTRelativeSlot(
        0, flatbuffers.number_types.UOffsetTFlags.py_type(value), 0
    )


def StringEnd(builder):
    return builder.EndObject()
