class SQLBased:
    @staticmethod
    def handle_aggregations(type, features):
        return ", ".join(
            [
                f"{aggregation}({type}.{feature}) as {feature}_{aggregation}"
                for feature in features
                for aggregation in features[feature]["aggregations"]
            ]
        )

    @staticmethod
    def parse_get(handle_aggregations_fn, get, vector_table_name="vector", raster_table_name="raster"):
        vector = []
        raster = []
        if "vector" in get:
            for feature in get["vector"]:
                if isinstance(feature, dict):
                    vector.append(handle_aggregations_fn("vector", feature))
                else:
                    vector.append(f"{vector_table_name}.{feature}")
            vector = ", ".join(vector)
        else:
            vector = ""
        if "raster" in get:
            for feature in get["raster"]:
                if isinstance(feature, dict):
                    raster.append(handle_aggregations_fn("raster", feature))
                else:
                    raster.append(f"{raster_table_name}.{feature}")
            raster = ", ".join(raster)
        else:
            raster = ""
        raster = f", {raster}" if raster else ""
        return f"select {vector} {raster}"

    @staticmethod
    def parse_join(join):
        table_ras = "{self.table_vec}" + f' as vector'
        table_vec = "{self.table_ras}" + f' as raster'
        condition = f'on {join["condition"]}'
        return f"from {table_ras} JOIN {table_vec} {condition}"

    @staticmethod
    def parse_condition(condition, vector_table_name="vector", raster_table_name="raster"):
        vector = SQLBased.build_condition(condition["vector"], vector_table_name, "and") if condition.get("vector") else ""
        raster = SQLBased.build_condition(condition["raster"], raster_table_name, "and") if condition.get("raster") else ""

        if vector and raster:
            return f"where {vector} and {raster}"
        elif vector and not raster:
            return f"where {vector}"
        elif not vector and raster:
            return f"where {raster}"
        elif not vector and not raster:
            return f""

    @staticmethod
    def build_condition(condition, table_name, operator):
        if isinstance(condition, str):
            return f"{table_name + '.' if table_name else ''}{condition}"
        if isinstance(condition, list):
            result = ""
            for idx, c in enumerate(condition):
                 result += f"{SQLBased.build_condition(c, table_name, operator)} {operator if idx + 1 < len(condition) else ''} "
            return f"{result}"
        if isinstance(condition, dict):
            if len(condition) > 1:
                raise ValueError("Only one condition is allowed")
            for o, c in condition.items():
                return f"({SQLBased.build_condition(c, table_name, o)})"


    @staticmethod
    def parse_group(group, vector_table_name="vector", raster_table_name="raster"):
        vector = (
            ", ".join([f"{vector_table_name}." + feature for feature in group["vector"]])
            if "vector" in group
            else ""
        )
        raster = (
            ", ".join([f"{raster_table_name}." + feature for feature in group["raster"]])
            if "raster" in group
            else ""
        )
        raster = f", {raster}" if raster else ""
        return f"group by {vector} {raster}"

    @staticmethod
    def parse_order(order, vector_table_name="vector", raster_table_name="raster"):
        vector = (
            ", ".join([f"{vector_table_name}." + feature for feature in order["vector"]])
            if "vector" in order
            else ""
        )
        raster = (
            ", ".join([f"{raster_table_name}." + feature for feature in order["raster"]])
            if "raster" in order
            else ""
        )
        raster = f", {raster}" if raster else ""
        return f"order by {vector} {raster}"
