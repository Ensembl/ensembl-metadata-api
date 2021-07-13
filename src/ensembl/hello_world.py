#  See the NOTICE file distributed with this work for additional information
#  regarding copyright ownership.
#
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Hello world module.

If executed as __main__ it will print "Hello world!" on
stdout.
"""


def hello(word: str) -> str:
    """Concats "Hello " with another string.

    Args:
        word (str): A string that is concatenated with "Hello "

    Returns:
        A string that is the result of the concatenation between "Hello " and `word`
    """
    return f"Hello {word}"


if __name__ == "__main__":
    hello("world!")
