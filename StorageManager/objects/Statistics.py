class Statistics:
    def __init__(self, n_r: int, b_r: int, l_r: int, f_r: int, V_a_r: dict[str, int]) -> None:
        """
        n_r: number of tuples in relation r
        b_r: number of blocks containing tuples of r
        l_r: size of a tuple in relation r (bytes)
        f_r: blocking factor (number of tuples that fit into one block)
        V_a_r: dictionary where key is attribute name, and value is number of distinct values for that attribute
        """
        self.n_r = n_r
        self.b_r = b_r
        self.l_r = l_r
        self.f_r = f_r
        self.V_a_r = V_a_r

    def __repr__(self) -> str:
        return (f"Statistics(n_r={self.n_r}, b_r={self.b_r}, l_r={self.l_r}, "
                f"f_r={self.f_r}, V_a_r={self.V_a_r})")
