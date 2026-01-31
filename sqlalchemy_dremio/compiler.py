# In sqlalchemy_dremio/compiler.py
from sqlalchemy.sql import compiler

class DremioCompiler(compiler.SQLCompiler):
    def visit_function(self, func, **kw):
        name = func.name.lower()

        # 1. Fix incorrect DATEDIFF (Date - Integer) translation
        if name == 'datediff':
            clauses = func.clauses.clauses
            # If DATEDIFF is called with (Date, Integer), it should be DATE_SUB
            return "DATE_SUB(%s, %s)" % (
                self.process(clauses[0], **kw),
                self.process(clauses[1], **kw)
            )

        # 2. Ensure DATE_ADD is handled natively
        if name == 'date_add':
            clauses = func.clauses.clauses
            return "DATE_ADD(%s, %s)" % (
                self.process(clauses[0], **kw),
                self.process(clauses[1], **kw)
            )

        # 3. Ensure DATE_SUB is handled natively
        if name == 'date_sub':
            clauses = func.clauses.clauses
            return "DATE_SUB(%s, %s)" % (
                self.process(clauses[0], **kw),
                self.process(clauses[1], **kw)
            )

        return super(DremioCompiler, self).visit_function(func, **kw)
