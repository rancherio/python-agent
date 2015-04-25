from cattle.type_manager import register_type, LIFECYCLE

from .volmgr import Volmgr

register_type(LIFECYCLE, Volmgr())
