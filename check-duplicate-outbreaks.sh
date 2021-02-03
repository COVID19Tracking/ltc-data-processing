#!/bin/bash

# AR
echo "Checking AR"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1ipN1gD0dteL6PizFWD3X968fGbGXlRd4Tyr6zzigPz0/edit#gid=881981274" -o outputs/AR_duplicate_outbreaks.csv

# CA
echo "Checking CA"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1JdKLbdie-TxGRvDwSPZsXk5dE06wlxqXQOE0UNxEvwE/edit#gid=507666539" -o outputs/CA_duplicate_outbreaks.csv

# CO
echo "Checking CO"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1NkOB-mTv2M29YNg6VNvmDEG8A2aZ0xVPPzxjlVS86a0/edit#gid=1994453153" -o outputs/CO_duplicate_outbreaks.csv

# CT
echo "Checking CT"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1zBc_kBX4Dn2fDoR52wRmnYshMZ8zJ_dgpbjLoYl1XEw/edit#gid=256788853" -o outputs/CT_duplicate_outbreaks.csv

# DC
echo "Checking DC"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1vZYb2V6oU3W-Gu05xjNCAd9_OLj93x0J_69D8vu2AxA/edit#gid=1442294420" -o outputs/DC_duplicate_outbreaks.csv

# DE
echo "Checking DE"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1xrv-gOZim7fx2jDpGpY83mTzCGxYJ9ioiHZl4NG-T9g/edit#gid=958546495" -o outputs/DE_duplicate_outbreaks.csv

# FL
echo "Checking FL"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1JAp8xj6yqbFLnEt8XoM2ybdwNZxM7RBpGPuJr4PP7x0/edit#gid=0" -o outputs/FL_duplicate_outbreaks.csv

# GA
echo "Checking GA"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1hQaLPKGvmtxFxDTtaV7FAglIoXwmhhlBr8CYDEhNs7M/edit#gid=392140376" -o outputs/GA_duplicate_outbreaks.csv

# HI
echo "Checking HI"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1CrwYQSKTFJLyJ-Zh85WEd4jIU13GdbjJ2TxBfEv00NQ/edit#gid=84490968" -o outputs/HI_duplicate_outbreaks.csv

# IA
echo "Checking IA"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/17sAQZ0wCoyhoBi5yaBcwqUYnOXN5R1jj6KdwIYk4pBY/edit#gid=384463153" -o outputs/IA_duplicate_outbreaks.csv

# ID
echo "Checking ID"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1XtDBmyyqeEH_qVtRl-yMSJc9W32Jcx9UhuUkEV-n6-Y/edit#gid=763789076" -o outputs/ID_duplicate_outbreaks.csv

# IL
echo "Checking IL"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1Aph2HoNP2uoz7D0G5R5FioBBTgC4lEYCuTKdo4H0OlA/edit#gid=273523772" -o outputs/IL_duplicate_outbreaks.csv

# IN
echo "Checking IN"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1HtKUm7zZpRhqGmHwrud37MiIlxEVZKF-yrZenSIYlUk/edit#gid=324533025" -o outputs/IN_duplicate_outbreaks.csv

# KS
echo "Checking KS"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1_7zcu-3YtE916wLIcfvmpYbZw_GbRwluUMeglzPHCrk/edit#gid=552036390" -o outputs/KS_duplicate_outbreaks.csv

# KY
echo "Checking KY"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/163HndcXbSYCQCeyEQheK7sUosqjDsh_lsp1Mav7d60A/edit#gid=1379565237" -o outputs/KY_duplicate_outbreaks.csv

# LA
echo "Checking LA"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1Sc19rZHqWLy0gjnB44wREEUsdR091_fw9AxOh7lpPdk/edit#gid=753014965" -o outputs/LA_duplicate_outbreaks.csv

# MA
echo "Checking MA"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1ffbPFQgzj94D0fuujwjSy2W3NXUHxT3zXXPHBk4t6YY/edit#gid=1423488567" -o outputs/MA_duplicate_outbreaks.csv

# MD
echo "Checking MD"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/117Z7IqF-m1pm2pQP15qHR79ot2DMzCL8hXYZiWTNFhc/edit#gid=144695144" -o outputs/MD_duplicate_outbreaks.csv

# ME
echo "Checking ME"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1oMJHd7Jub6Qtv-fXKYeMbBZJOGmZOVJIUHVSWfa88X0/edit#gid=241350954" -o outputs/ME_duplicate_outbreaks.csv

# MI
echo "Checking MI"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/17SdZ-I-R7B-R1V1wvOWiobSHjHUBh2yqUN2ksogGTPk/edit#gid=1609250679" -o outputs/MI_duplicate_outbreaks.csv

# MN
echo "Checking MN"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1VabnwGtQtYA0ItVHmRj5XDvISxYYpQ5yRqRj8WEIFUM/edit#gid=533017060" -o outputs/MN_duplicate_outbreaks.csv

# MO
echo "Checking MO"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1-L9SdLpNdJAuZrayZfR32Z-q7HHp0SvOCCFZny3WLLU/edit#gid=0" -o outputs/MO_duplicate_outbreaks.csv

# MS
echo "Checking MS"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1fZva-jL8cx8sFG588Hms04PPpmjsdEwpn2KAnHpWNzU/edit#gid=262395853" -o outputs/MS_duplicate_outbreaks.csv

# NC
echo "Checking NC"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1XZCYOsp0Gjxw9YuQhkJHYgU3_Ssn1E6RioS1eseLwVA/edit#gid=0" -o outputs/NC_duplicate_outbreaks.csv

# ND
echo "Checking ND"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1YEufZtW-XbpT3LRKQa909Vmg4Ejjb2dyyYCcc_tPN1U/edit#gid=1764761535" -o outputs/ND_duplicate_outbreaks.csv

# NJ
echo "Checking NJ"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1hMUUloAzzyLYGJWENtATpP3KyqqmlShVsBWL5L5GBHw/edit#gid=1050687942" -o outputs/NJ_duplicate_outbreaks.csv

# NM
echo "Checking NM"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1H-R9ppYlmbFJSKCGGR9FlzojOUrt9Lj-4nc2nUVNYog/edit#gid=1593753065" -o outputs/NM_duplicate_outbreaks.csv

# NV
echo "Checking NV"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1XLVTsMhRcfOsI4hy5KHhWMhK3xerjgv2zkAHMjYENbU/edit#gid=0" -o outputs/NV_duplicate_outbreaks.csv

# NY
echo "Checking NY"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1kBBS5qD5G2IPgBsOBokHXik0LumsLE6Td2LcmMbrlOY/edit#gid=2023625666" -o outputs/NY_duplicate_outbreaks.csv

# OH
echo "Checking OH"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1Jc1glw3QDD4KKx2IsZRAnG3olMBJCCPJjlItvTYWKuw/edit#gid=0" -o outputs/OH_duplicate_outbreaks.csv

# OK
echo "Checking OK"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1cBm_kXKs5vlIOt7sFJwe3qAu45B1xoahRTB8BCwXxD4/edit#gid=597002417" -o outputs/OK_duplicate_outbreaks.csv

# OR
echo "Checking OR"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1FPEJYphwBx0jMTJ8V5UfhJroHlaRW0CUqhUVJTjgidk/edit#gid=76191343" -o outputs/OR_duplicate_outbreaks.csv

# PA
echo "Checking PA"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1wujLzyF0qVxMcbEHA4ukQf0GIjisdTom2mww7JXDhjg/edit#gid=0" -o outputs/PA_duplicate_outbreaks.csv

# RI
echo "Checking RI"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1x3d0A4LRUImeCN-ElcILiQoAfd_XadAkvsHqq5KSONA/edit#gid=1363217135" -o outputs/RI_duplicate_outbreaks.csv

# SC
echo "Checking SC"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/16RdP2Pnzhwn700kEVREoTg9g2SaNKoKdTf7MKJfmb08/edit#gid=1883932637" -o outputs/SC_duplicate_outbreaks.csv

# TN
echo "Checking TN"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/11ZTR7eBvctlJ1X4WyNm7Haikku1Dy8Yw79P1y-KEQcw/edit#gid=686454990" -o outputs/TN_duplicate_outbreaks.csv

# TX
echo "Checking TX"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1lLcxGG1nhe_EapnkEv6J3dlCTXNprEAz57emDskLYyQ/edit#gid=656760829" -o outputs/TX_duplicate_outbreaks.csv

# UT
echo "Checking UT"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1fE4681oQr9SubYavvP23LtWiaSLCjcU2-v6-hqoyKHA/edit#gid=1053026410" -o outputs/UT_duplicate_outbreaks.csv

# VA
echo "Checking VA"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1LA3acewYNlk6P9frvC1e8r6wZjXT3omleVxzNC3QITA/edit#gid=1235171997" -o outputs/VA_duplicate_outbreaks.csv

# VT
echo "Checking VT"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/17SuSHsTkNoEerbXDQhLmhibqGV20aTy-DQqPpaTL84o/edit#gid=0" -o outputs/VT_duplicate_outbreaks.csv

# WI
echo "Checking WI"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1XTS_MNPhR5mLczE8wlK8lDMSHLzwPl1ZHJVzYhUI2GM/edit#gid=1717442458" -o outputs/WI_duplicate_outbreaks.csv

# WV
echo "Checking WV"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1omhRJvotgETaeUJxmDWiBkS2ae-BIa-fFJUvodsd88U/edit#gid=2117780908" -o outputs/WV_duplicate_outbreaks.csv

# WY
echo "Checking WY"
FLASK_APP=flask_server.py flask quality_checks "https://docs.google.com/spreadsheets/d/1STMjbR4OY36cSVokYFjJkeHxgSfyMDipsSt4gPBGEeQ/edit#gid=1493722885" -o outputs/WY_duplicate_outbreaks.csv
