import geoip2.database
import geoip2.errors

from apachelogs import LogParser, InvalidEntryError, parse_apache_timestamp

from Common import asn_db, country_db


def parse_log(log_line):
    """
    function to parse a log line
    :param log_line: a simple line of apache log line
    :return: return a tuple with the data parsed
    """
    from Common import log_format

    parser = LogParser(log_format)
    try:
        log = parser.parse(log_line)
        return (
            log.directives["%h"],
            log.directives["%t"],
            log.directives["%r"],
            log.directives["%>s"],
            log.directives["%b"],
            "Empty" if log.directives["%{Referer}i"] is None else log.directives["%{Referer}i"] ,
            "Empty" if log.headers_in["User-Agent"] is None else log.headers_in["User-Agent"]
        )
    except InvalidEntryError :
        log_split = log_line.split(" ")
        return (
            log_split[0],
            parse_apache_timestamp(log_split[3]+" "+log_split[4]),
            "ERROR",
            -1,
            -1,
            "ERROR",
            "ERROR"
        )

def retrieve_asn(ip):
        try:
            asn = asn_db.asn(ip).autonomous_system_organization
        except geoip2.errors.AddressNotFoundError:
            asn = "Empty"
        return asn

def retrieve_country(ip):
        try:
            country = country_db.country(ip).country.name
        except geoip2.errors.AddressNotFoundError:
            country = "Empty"
        return country
