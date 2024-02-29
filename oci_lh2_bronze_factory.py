from inspect import getmembers, isclass, isabstract, ismodule
from nlsdata.oic_lh2_bronze import *
import nlsdata.oic_lh2_bronze
from nlstools.tool_kits import *

PACKAGE = nlsdata.oic_lh2_bronze
ABSTRACTCLASS = oci_lh2_bronze.BronzeSourceBuilder

class NLSDataBronzeFactory():
  db_wrappers = {}

  def __init__(self,mapping_class=None):
    self.load_db_wrappers()
    if mapping_class:
      self.db_wrappers = { k:self.db_wrappers[mapping_class[k]] for k in mapping_class.keys()}

  def load_db_wrappers(self):
    modules = [(name,obj) for name,obj in getmembers(PACKAGE,lambda m: ismodule(m))]
    for module in modules:
      classes = getmembers(module[1], lambda m: isclass(m) and not isabstract(m))
      for name, _type in classes:
        if isclass(_type) and issubclass(_type, ABSTRACTCLASS):
          self.db_wrappers.update([[name, _type]])

  def create_instance(self, wrapper_name,*args,**kwargs):
    if wrapper_name in self.db_wrappers:
      return self.db_wrappers[wrapper_name](*args,**kwargs)
    else:
      return None