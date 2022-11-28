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
    def parse_get(handle_aggregations_fn, get):
        vector = []
        raster = []
        if "vector" in get:
            for feature in get["vector"]:
                if isinstance(feature, dict):
                    vector.append(handle_aggregations_fn("vector", feature))
                else:
                    vector.append(f"vector.{feature}")
            vector = ", ".join(vector)
        else:
            vector = ""
        if "raster" in get:
            for feature in get["raster"]:
                if isinstance(feature, dict):
                    raster.append(handle_aggregations_fn("raster", feature))
                else:
                    raster.append(f"raster.{feature}")
            raster = ", ".join(raster)
        else:
            raster = ""
        raster = f", {raster}" if raster else ""
        return f"select {vector} {raster}"

    @staticmethod
    def parse_join(join):
        table1 = "{self.table1}" + f' as {join["table1"]}'
        table2 = "{self.table2}" + f' as {join["table2"]}'
        condition = f'on {join["condition"]}'
        return f"from {table1} JOIN {table2} {condition}"

    @staticmethod
    def parse_condition(condition):
        vector = (
            "and ".join(["vector." + feature for feature in condition["vector"]])
            if "vector" in condition
            else ""
        )
        raster = (
            "and ".join(["raster." + feature for feature in condition["raster"]])
            if "raster" in condition
            else ""
        )

        if vector and raster:
            return f"where {vector} and {raster}"
        elif vector and not raster:
            return f"where {vector}"
        elif not vector and raster:
            return f"where {raster}"
        elif not vector and not raster:
            return f""

    @staticmethod
    def parse_group(group):
        vector = (
            ", ".join(["vector." + feature for feature in group["vector"]])
            if "vector" in group
            else ""
        )
        raster = (
            ", ".join(["raster." + feature for feature in group["raster"]])
            if "raster" in group
            else ""
        )
        raster = f", {raster}" if raster else ""
        return f"group by {vector} {raster}"

    @staticmethod
    def parse_order(order):
        vector = (
            ", ".join(["vector." + feature for feature in order["vector"]])
            if "vector" in order
            else ""
        )
        raster = (
            ", ".join(["raster." + feature for feature in order["raster"]])
            if "raster" in order
            else ""
        )
        raster = f", {raster}" if raster else ""
        return f"order by {vector} {raster}"
