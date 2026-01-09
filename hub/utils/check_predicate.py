import cpmpy as cp
import sqlglot
from cpmpy import intvar
from cpmpy.expressions.core import Comparison, Operator

end_expr = {
    sqlglot.exp.GT: ">",
    sqlglot.exp.LT: "<",
    sqlglot.exp.EQ: "==",
    sqlglot.exp.GTE: ">=",
    sqlglot.exp.LTE: "<=",
    sqlglot.exp.NEQ: "!=",
}

op_expr = {
    sqlglot.exp.And: "and",
    sqlglot.exp.Or: "or",
    sqlglot.exp.Not: "not",
}


class ASTQuery:
    def __init__(self, vector_meta: dict, query: str) -> None:
        self.query = f"select * from vector where {query}" if query else "select * from vector"
        self.ast = sqlglot.parse_one(self.query or "")

        self.str_value_map = {}

        self.fields = vector_meta['layers'][0]['fields']
        self.field_types = {field['name']: field['type'] for field in self.fields}

    def create_str_value_map(self, base_dict: dict = None):
        str_values = [l.this for l in self.ast.find_all(sqlglot.exp.Literal) if l.is_string]
        str_values = list(set(str_values))

        if base_dict is None:
            base_dict = {}

        str_value_map_add = { v: i + len(base_dict) for i, v in enumerate(str_values) }
        self.str_value_map = base_dict | str_value_map_add

    def where_to_linear_program(self):
        where = self.ast.find(sqlglot.exp.Where)

        def ast_to_linear_program(ast):
            print(ast)
            left = ast.this
            right = ast.expression

            match type(ast):
                case expr if expr in end_expr.keys():
                    return Comparison(end_expr[type(ast)], self.build_term(left),
                                      self.build_term(right))
                case sqlglot.exp.In:
                    values = [self.build_term(value) for value in ast.expressions]
                    field = self.build_term(left)
                    return Operator("or", [Comparison("==", field, value) for value in values])
                # case _:
                #     for condition in ast.iter_expressions():
                #         left = condition.left
                #         right = condition.right
                #         param_type = type(condition)
                #
                #         match param_type:
                case sqlglot.exp.And:
                    return Operator("and", [ast_to_linear_program(left), ast_to_linear_program(right)])
                case sqlglot.exp.Or:
                    return Operator("or", [ast_to_linear_program(left), ast_to_linear_program(right)])
                case sqlglot.exp.Not:
                    return Operator("not", [ast_to_linear_program(left)])
                case _:
                    raise NotImplementedError(f"Condition type {type(ast)} not supported.")

        if not where:
            return None
        return ast_to_linear_program(where.this)

    def build_term(self, term):
        match type(term):
            case sqlglot.exp.Column:
                field_name = term.name
                field_type = self.field_types[field_name]
                if field_type in ["Integer", "Real"]:
                    return intvar(-9999999, 9999999, name=field_name)
                elif field_type == "String":
                    return intvar(0, len(self.str_value_map), name=field_name)  # Placeholder for string handling
                else:
                    raise NotImplementedError(f"Field type {field_type} not supported.")
            case sqlglot.exp.Literal:
                if term.is_string:
                    return self.str_value_map[term.this]
                else:
                    return int(term.this)
            case _:
                raise NotImplementedError(f"Term type {type(term)} not supported.")


    def check_implies(self, other_query: 'ASTQuery') -> bool:
        """
        Check if query q1 implies query q2 using CP-SAT solver.
        :param other_query: The ASTQuery object representing query q2
        :return: True if q1 implies q2, False otherwise
        """

        self.create_str_value_map()
        lp_base = self.where_to_linear_program()

        if lp_base is None:
            return True

        other_query.create_str_value_map(self.str_value_map)
        lp_implied = other_query.where_to_linear_program()

        if lp_implied is None:
            return False

        model = cp.Model(Operator("not", [lp_base]), lp_implied)
        return not model.solve(solver="exact")

    def check_equals(self, other_query: 'ASTQuery') -> bool:
        """
        Check if query q1 is equivalent to query q2 using CP-SAT solver.
        :param other_query: The ASTQuery object representing query q2
        :return: True if q1 is equivalent to q2, False otherwise
        """

        self.create_str_value_map()
        lp_base = self.where_to_linear_program()

        other_query.create_str_value_map(self.str_value_map)
        lp_implied = other_query.where_to_linear_program()

        if lp_base is None and lp_implied is None:
            return True
        elif lp_base is None or lp_implied is None:
            return False

        model = cp.Model(Comparison("==", lp_base, lp_implied))
        return model.solve(solver="exact")




# class CheckPredicate:
#     def __init__(self, vector_meta: dict, query_base: str, query_implied: str) -> None:
#         self.vector_meta = vector_meta
#         self.query_base = query_base
#         self.query_implied = query_implied
#
#         self.ast_base = sqlglot.parse_one(self.query_base)
#         self.ast_implied = sqlglot.parse_one(self.query_implied)
#
#         str_values = [l.this for ai in [self.ast_base, self.ast_implied] for l in ai.find_all(sqlglot.exp.Literal) if l.is_string]
#         str_values = list(set(str_values))
#         self.str_value_map = { v: i for i, v in enumerate(str_values) }
#
#         self.fields = vector_meta['layers'][0]['fields']
#         self.field_types = {field['name']: field['type'] for field in self.fields}
#
#         self.lp_base = self.where_to_linear_program(self.ast_base)
#         self.lp_implied = self.where_to_linear_program(self.ast_implied)
#



