from inspect import getmembers, isclass, isabstract, ismodule
from nlsdata.oci_lh2_bronze import *
import nlsdata.oci_lh2_bronze
from nlstools.tool_kits import *

PACKAGE = nlsdata.oci_lh2_bronze
ABSTRACTCLASS = oci_lh2_bronze.BronzeSourceBuilder

class NLSDataBronzeFactory():
  lh2_bronze_wrappers = {}

  def __init__(self,mapping_class=None):
    self.load_db_wrappers()
    if mapping_class:
      self.lh2_bronze_wrappers = { k:self.lh2_bronze_wrappers[mapping_class[k]] for k in mapping_class.keys()}

  def load_db_wrappers(self):
    modules = [(name,obj) for name,obj in getmembers(PACKAGE,lambda m: ismodule(m))]
    for module in modules:
      classes = getmembers(module[1], lambda m: isclass(m) and not isabstract(m))
      for name, _type in classes:
        if isclass(_type) and issubclass(_type, ABSTRACTCLASS):
          self.lh2_bronze_wrappers.update([[name, _type]])

  def create_instance(self, wrapper_name,*args,**kwargs):
    if wrapper_name in self.lh2_bronze_wrappers:
      return self.lh2_bronze_wrappers[wrapper_name](*args,**kwargs)
    else:
      return None