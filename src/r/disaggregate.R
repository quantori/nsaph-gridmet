
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
