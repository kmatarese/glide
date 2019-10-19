from glide.core import Node


class Average(Node):
    """Push the average of the input"""

    def run(self, data):
        """Take the average of data and push it"""
        self.push(sum(data) / len(data))


class Sum(Node):
    """Push the sum of the input"""

    def run(self, data):
        """Take the sum of data and push it"""
        self.push(sum(data))
