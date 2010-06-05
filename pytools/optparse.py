
def vararg_callback(option, opt_str, value, parser):
     assert value is None
     value = []

     def floatable(str):
         try:
             float(str)
             return True
         except ValueError:
             return False

     for arg in parser.rargs:
         # stop on --foo like options
         if arg[:2] == "--" and len(arg) > 2:
             break
         # stop on -a, but not on -3 or -3.0
         if arg[:1] == "-" and len(arg) > 1 and not floatable(arg):
             break
         value.append(arg)

     del parser.rargs[:len(value)]
     setattr(parser.values, option.dest, value)
