#  Copyright (c) 2021. Harvard University
#
#  Developed by Research Software Engineering,
#  Faculty of Arts and Sciences, Research Computing (FAS RC)
#  Author: Ben Sabath
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

function(x, fact) {
    out <- raster(x)

	ncx <- ncol(x)
	nrx <- nrow(x)
	dim(out) <- c(nrx * fact, ncx * fact)
	names(out) <- names(x)

    x <- getValues(x)
    cols <- rep(seq.int(ncx), each=xfact)
    rows <- rep(seq.int(nrx), each=yfact)
    cells <- as.vector( outer(cols, ncx*(rows-1), FUN="+") )
    x <- x[cells]
    out <- setValues(out, x)
	return(out)
}
