time.of.day <- function(strg)
{
    h <- strsplit(strg," ")[[1]][2]
    if (is.na(h)) return(NA)
    h <- gsub(":.*", "", h)
    h <- gsub("^0+([1-9])", "\\1", h)
    h <- as.numeric(h)

    if ((h>=7) & h<=12) {
        return ("Morning")
    } else if ((h>12) & (h<=17)) {
        return("Afternoon")
    } else if ((h>17) & (h <=22) ) {
        return("Evening")
    } else if ((h==23) | (h == 0) | (h < 7) ) {
        return("Night")
    }

}
