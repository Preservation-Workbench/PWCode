# Copyright(C) 2022 Morten Eek

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
from pathlib import Path
import shutil
from argparse import ArgumentParser, SUPPRESS
from dataclasses import dataclass

from rich.traceback import install
from rich_argparse import RawTextRichHelpFormatter
import gui
import config
from utils import _java
import _copy
import _archive
import _script


# def get_log_file(args):
# args.log_dir = Path(args.tmp_dir, "logs")
# pw_file = import_file(str(Path(LIB_DIR, "file.py")))
# pw_log = import_file(str(Path(LIB_DIR, "log.py")))
# log_file = pw_file.uniquify(Path(args.log_dir, args.target_name + ".log"))
# pw_log.configure_logging(log_file)
#
# return log_file


def ensure_args_attr(attrs, args):
    for attr in attrs:
        if not hasattr(args, attr):
            setattr(args, attr, False)

    return args


def get_args(argv):
    if len(argv) == 1 or (len(argv) == 2 and any(x in ["copy", "archive", "script"] for x in argv)):
        argv.append("--help")

    common_parser = ArgumentParser("common", add_help=False)

    if argv[1] in ["copy"]:
        files = ["tables", "json", "ddl", "copy"]
        common_parser.add_argument(
            "--stop",
            dest="stop",
            metavar="FILE",
            choices=files,
            help="Stop after generating <"
            + ",".join(files)
            + ">-file and open for editing (triggers regeneration if rerun).",
        )
        common_parser.add_argument("--debug", dest="debug", action="store_true", help="Show debug messages.")
        common_parser.add_argument(
            "--test",
            dest="test",
            action="store_true",
            help="Test run: Copied data are subsequently deleted and empty target tables are always recreated.",
        )

    if argv[1] in ["archive"]:
        common_parser.add_argument(
            "--target",
            dest="target",
            type=str,
            help="AIP directory path or project name. Reuses --source if empty.",
        )

    common_parser.add_argument(
        "--help",
        action="help",
        default=SUPPRESS,
        help="Show this help message and exit.",
    )
    common_parser._optionals.title = "Optional"

    # Main:
    parser = ArgumentParser(
        prog="pwetl",
        add_help=False,
        description="Program to copy, export and normalize data. Run subcommands for more information.",
        formatter_class=RawTextRichHelpFormatter,
        parents=[common_parser],
    )

    parser._positionals.title = "Command"

    # Commands:
    subparsers = parser.add_subparsers(help="", dest="command")

    # Copy:
    copy_parser = subparsers.add_parser(
        "copy",
        add_help=False,
        description="Copy database data.",
        epilog="".join(
            (
                "Files:\n",
                "  config: Config file for connctions/aliases, database drivers etc.\n",
                "  tables: List of tables to export or copy.\n",
                "  json:   JSON schema of data package with tables in dependency order.\n",
                "  ddl:    SQL code generated from JSON schema to recreate source schema.\n",
                "  copy:   Generated statements for copying data from source to target.\n",
                "\nExample of usage:\n",
                '  pwetl copy --source "jdbc:sqlite:/home/pwb/source.db" --target "jdbc:sqlite:/home/pwb/target.db"\n',
            )
        ),
        formatter_class=RawTextRichHelpFormatter,
        parents=[common_parser],
    )
    if argv[1] == "copy":
        copy_arguments = copy_parser.add_argument_group("Arguments")
        copy_arguments.add_argument(
            "--source", dest="source", type=str, required=True, help="Database jdbc url/alias or directory."
        )
        copy_arguments.add_argument(
            "--target",
            dest="target",
            type=str,
            required=True,
            help="Database jdbc url/alias, directory path or project name (creates a SIP if path or project).",
        )
        copy_parser._optionals.title = "Optional"
        copy_parser._action_groups.reverse()

    # archive:
    archive_parser = subparsers.add_parser(
        "archive",
        add_help=False,
        description="Archive data as OAIS datapackage",
        formatter_class=RawTextRichHelpFormatter,
        parents=[common_parser],
    )

    archive_arguments = archive_parser.add_argument_group("Arguments")
    archive_arguments.add_argument(
        "--source", dest="source", type=str, required=True, help="SIP directory path or project name."
    )
    archive_parser._optionals.title = "Optional"
    archive_parser._action_groups.reverse()

    # Configure:
    config_parser = subparsers.add_parser(
        "configure",
        add_help=False,
        description="".join(("Configure program by editing config.yml.",)),
        formatter_class=RawTextRichHelpFormatter,
        parents=[common_parser],
    )

    # Script:
    script_parser = subparsers.add_parser(
        "script",
        add_help=False,
        description="Run custom python script.",
        formatter_class=RawTextRichHelpFormatter,
        parents=[common_parser],
    )
    script_arguments = script_parser.add_argument_group("Script")
    script_arguments.add_argument("--path", dest="path", type=str, required=True, help="Path to custom script.")

    args = ensure_args_attr(["stop", "debug", "test", "source", "target", "path"], parser.parse_args())

    cfg_file = Path(Path(__file__).resolve().parents[1], "config.yml")
    if not Path(cfg_file).is_file():
        shutil.copy(Path(cfg_file.parent, "config.template.yml"), cfg_file)

    login_alias, jdbc_drivers, jar_files = config.load(cfg_file)

    return config.Main(
        cfg_file=cfg_file,
        command=args.command,
        script_path=args.path,
        debug=args.debug,
        stop=args.stop,
        test=args.test,
        source=args.source,
        target=args.target,
        login_alias=login_alias,
        jdbc_drivers=jdbc_drivers,
        jar_files=jar_files,
    )


# --> ikke ta vare på gamle filer->de skal inn i subversion som første commit før konvertering heller
# --> Filbaner i {schema1}-documents.db og documents.db må være relative ift hvor databasene er selv -> er det i PWConvert?
# --> Må legge inn støtte for visse subversion-kommandoer
# SIP/AIP/DIP:
# - content.txt
# - content/
#    - {schema1}/
#       - datapackage.json
#       - ddl.sql
#       - data/
#           - table1.tsv
#           - table2.tsv
#           - table3.tsv
#       - documents/
#           - file1.pdf
#           - file2.txt
#           - file3.md
#    - documents/
#       - {file-source1}
#       - {file-source2}
#    - documents.db
#    - {schema2}/
#       - datapackage.json
#       - ddl.sql
#       - data/
#           - table1.tsv
#           - table2.tsv
#           - table3.tsv
#       - documents/
#           - file1.pdf
#           - file2.txt
#           - file3.md
#    - dip.sql
# - descriptive-metadata/
#    - eac.xml
#    - ead.xml
#    - readme.md


def run(argv):
    install(show_locals=False)  # Better debugging
    main_cfg = get_args(argv)

    #print(main_cfg.pwxtract_dir)
    #print(main_cfg.jars_dir)
    #sys.exit()
    _java.init_jvm(main_cfg.java_home, main_cfg.jars_dir)

    # args.log_file = get_log_file(args)
    # TODO: Bytt ut log-løsning i linje over. Ha mappe for log som ikke er avhengig av project?


    cmds = {
        "configure": (lambda x: gui.show_output(main_cfg, main_cfg.cfg_file)),
        "copy": (lambda x: _copy.run(main_cfg)),
        "archive": (lambda x: _archive.run(main_cfg)),
        "script": (lambda x: _script.run(main_cfg)),
    }

    return cmds[main_cfg.command](main_cfg)

    # TODO: For BKSAK: Trenger arg for copy på å utelate blob'er (til tekst-felt med ref som i archive da) -> gjør før bksak-uttrekk
    # -> stopp etter generert copy statements for å sjekk hva som utelates da

    # TODO: Skriv original databasetype til JSON

    # TODO: Zippet build til mappe release under mappe build?

    # TODO: Endre til å bruke tmp_dir under projects direkte heller enn en pr project

    # TODO: Maven bør også hentes fra config heller enn å være angitt direkte i koden

    # TODO: Er raskere å slette opprinnelig kopiert tabell i sqlite som har constraints og så kopiere den på nytt enn å bruke sqlite_utils til å legge på fk!!!!

    # TODO: test for å få stdout/err fra jpype: https://github.com/search?q=repo%3Ajpype-project%2Fjpype%20%20contextlib&type=code

    # TODO: Hvis linebreaks i kolonne skal eksporteres som fil alltid? Flere kriterier?

    # TODO: Sjekk hvilken databaser som slipper inn null bytes . Bare fjern når original db tillater

    # TODO: Bruke denne så får sqlite med extensions? https://simonwillison.net/2023/Jun/17/sqleanpy/

    # TODO: Test til tsv hele base -> var det ikke felt som skulle skrives som filer til disk? -> test med annen base hvis ikke
    # -> sjekk først smvprod under PWCode

    # TODO: Må være feil i kolonne source_column_nullable i columns i configdb da den har verdi 1 for kolonner som er primary key

    # "count_of_rows" : 504, -> kan ha som egen i skjema heller enn i comment ser det ut til -> eller skrives den senere? Under validataion?
    # --> har lagt inn i kode -> sjekk i json og endre så til at leser der heller enn i comment etterpå

    # TODO: Test å bruke DISABLEOUT -> bør uansett alltid disable DBMS_OUTPUT for oracle når henter fra LONG og LONG RAW data types

    # TODO: Test archive_tar + gjør ferdig archive_dir

    # TODO: Lag fork med click/tui

    # TODO: Juster så ikke sqlwb sine logger legger seg i jars-mappe. Evt. sletter alle andre logger enn den som heter akkurat workbench.log
    # -> Også bruke Property: workbench.log.maxfilesize og  workbench.log.backup.count ?

    # TODO: Starte sqlwb i cli modus fra python? -> se https://www.sql-workbench.eu/manual/console-mode.html

    # TODO: Mulig å validere en og en tabell i datapackage? -> Trenger også å kule vekk tabeller uten feil fra stdout

    # Legg inn så kan eksportere til tsv direkte fra original db -> da må normalisering av tabell- og kolonenavn gjøres!!

    # TODO: Test at eksport av blober virker i archive_db

    # TODO: Trenger en sjekk i export_file_column på at ikke eksporterer tom fil hvis var ikke tom i db

    # TODO: Trenger db som clob'er og blob'er å teste export_table på!

    # # TODO: Fjern så mange config args som mulig -> de som er kun brukt i en modul slik som java_home etter at virker ellers

    # TODO: _data.py som inneholder både configdb og div dataclasses? Kutt alld def I configdb som ikke brukes mange steder?

    # TODO: Ha kun en configdb pr prosjekt. Legg til kolonne som angir "subprosjekt"

    # TODO: Løsning for å sjekke verdier class Config inkl at paths finnes! -> bruke check_paths def eller noe innebygget?

    # TODO: Dbo som dataclass generert fra def?

    # Bruke config dataclass heller i confirm så er mer ferdig filtert hvilken som skal vises?

    # TODO: Bruk denne for å verifisere at sqlite-base ikke har blitt endret: https://www.sqlite.org/dbhash.html

    # TODO: Når sikker sone git server ok: Installer på adm-sone, kopier til sikker, kjør git pull

    # TODO: Legg inn row_count i "description" i json fil -> kan da bruke json helelr enn config-db for normalisering! -> bedre?

    # TODO: Lag --format og --query args

    # TODO: Test _archive med annet target enn source -> sette args.project verdi til hva da og generelt?

    # Putt i core mappe det som ikke generelt nok til utils og heller ikke kaller til ting på nivå over; bare kalles til

    # TODO: Trengs sjekk for datofelt også i export_table?

    # TODO: Endre export/import av excel mm til sqlwb? https://www.sql-workbench.eu/manual/command-import.html#spreadsheet-import
    # TODO: Lag export_tsv tilsvarende export_xlsx og bruk nå command er archive (eller valg under copy?->senere)
    # --> Se om jdbc til csv kode i lwetl -> er for csv, xml, xlsx mm -> sjekk også blob-håndtering! -> se extract-images.py
    # --> pwetl archive --source "jdbc:sqlite:/home/pwb/source.db" --target <project_name> --debug --stop ddl

    # TODO: Kalle --tables for --filter heller og liste opp filer hvis ikke db-source?
    # -> endre get_include_tables til def filter_source og håndter filer i tilleg til db

    # TODO: Legg inn sletting av data i tabeller med target count mindre enn source men mer enn 0

    # TODO: Bruk rich.progress for tarfile-operasjoner. Annet?

    # TODO: Kan en gjøre pip install PWConvert heller enn subrepo? -> test

    # TODO: lag kode for kopiering av filer i mappe som ikke har database -> Lage json schema også for filuttrekk?
    # --> Flytte get_checksum hvor? -> må kutte bruk av common for å splitte ut PWETL til eget repo

    # Konverter alle tabulære filer til tsv og så åpne de som virtuelle tabeller i SQLite? -> trengs extension til sqlite da?

    # TODO: For install fra git server på offline-maskin må installer kunne hente deps fra alternativt lokalt sted

    # TODO: Ha kun en add_foreign_keys pr table (så ikke oppretter ny tabell flere ganger enn nødvendig)

    # TODO: Definere projects mappe hvordan så PWETL kan kjøres uten pwtext? -> pwetl-repo til pwtext-repo senere?->ja
    # Git init og kopier inn filer fra pwetl
    # Flytt arkivpakkestruktur til readme.md. Ha egen todo.md også?

    # TODO: Definer std farger mm for heading, info, error mm i gui

    # TODO: Legg inn støtte for flere mime_types!! -> csv m fl som støttet av petl
    # Juster mime_types liste til dict som cmds over og splitt ut til egne funksjoner som kalles

    # TODO: Lag egen tabell import _files for å kunne forbedre ensure_tables_from_files import_files i _sqlite

    # TODO: Splitt ut og gjøre PWETL public i påvente av vpn og git-server på adm og sikker sone

    # TODO: Se i Joplin mail og markdown etter kode for SQLite til tsv (eller helst jdbc til tsv så støtter alle?). Mail og?

    # TODO: Fjern --stop copy (forvirrende med to copy og er heller ikke aktuelt når source ikke db)

    # Test get_foreign_keys på sqlite-base med to composite fk på samme tabell
    # --> hvordan støtte i sqlite hvor ikke har fk_navn å gruppere på? Sjekk også sortering!

    # TODO: Å kjøre db.fix_fk må også være del av normalize subcommand! -> første der når starter med archive med sqlite-base i?

    # Export til tsv og excel -> se på disse:
    # https://github.com/ekansa/sqlite-dump-to-csv/blob/master/sqlite_dump.py
    # https://github.com/Zocozoro/python-sqlite-to-csv/blob/master/sqlite_to_csv.py
    # -> se i sammenheng med eksport til tsv for datapackage
    # --> export til tsv med jdbc heller? -> se lwetl først om har

    # TODO: Endre til å bruke rich og bruk heller enn loguru: https://github.com/PrivatePandaCO/pyloggor/blob/master/pyloggor/pyloggor.py

    # TODO: Test med source som path for å kunne normalisere flyttet uttrekk --> eller bare target da? Hva er mest intuitivt? --> eller --normalize i stedet for?
    # TODO: Forskjellige config-filer for kommandoer?

    # TODO: Endre flere filer som bare skal brukes fra annen kode til å ha underscore først (som _copy.py)

    # TODO: Bedre args:
    # - Usynlige args med underscore først?
    # - Dataclass? https://github.com/Shadowsight9/fucking-argparse

    # TODO: Script for å laste opp ny release på GitHub? gh kommando?
    # TODO: Kan en bruke rich til å liste opp filnavn i examples mappe? Kalle annet da?

    # TODO: Bruk kwargs som vist i ene svaret her: https://stackoverflow.com/questions/9539921/how-do-i-define-a-function-with-optional-arguments
    # TODO: List ut detaljer om hva som mangler når: "Something went wrong. Missing data in target!"
    # Fjern evt duplikatinformasjon configdb-tabeller så mest mulig normalisert
    # TODO: Ha pwtext.py. Flytt mer over fra bash og Powershell etterhvert. Når installasjon ok og ikke gui list subscripts som kan kjøres
    # TODO: Blir feil når kjører script som åpner cudatext og cudatext ikke åpnet allerede -> må åpnes med disown eller lignende!!
    # TODO: Legg inn i bash-script flytting av desktop-filer inkl fikse ikon for sqlwb
    # TODO: Noe duplikatinformasjon i configdb tabeller: row_count åp target/copied i source mm. Forenkle det!!
    # TODO: Legg inn i bash script at også mappe med sqlwb manual legges på plass i jar-mappe
    # TODO: Lagre kombinasjoner av args som kjørt tidligere til en config-fil så kan hentes opp som liste og velges blant senere->kan brukes av senere gui også
    # TODO: Fiks at desktop fil ikke virker i cinnamon->hvorfor ikke? (ser heller ikke ut til å finnes alternativ til desktop fil som gir ikon)
    # TODO: Må bytte ut jaydebeapi med jpype eller annet for å kunne kalle prosedyrer og pragma og få resultat: https://jpype.readthedocs.io/en/latest/dbapi2.html
    # TODO: Lag genererell plugin med gui som genererer felt for input pr cli arg + har rullegardingmeny over med tidligere input + kjører så i exterm
    # -> lage egen meny på øverste nivå slik som exttols plugin (Tools meny) -> se hvilken plugin som har gui som kan tas utgangspunkt i
    # TODO: cuda_exterminal: Fiks bakgrunn som ikke alltid themed! Test å endre navn mm til egen fork! Kombiner med runner plugin?
    # TODO: Mangler litt på constraints (inkl constraints innad i tabell) -> tester hvordan best gjøres for sqlite
    # TODO: Mulig å koble fra h2 db etter commit og wbdisconnect i enda større grad så ikke opplever problem med at new row count er feil?
    # TODO: Test kopiering fra sqlite til h2 med constraints! Så fra h2 til oracle med constrints! Så fra oracle til sqlite!
    # TODO: Fortsatt tilfeller når ikke har blitt skrevet deps til configdb -> finn hva som trigger det og fiks! Mulig bare når kjører kopiering på nytt
    # TODO: Hvis rerun med færre tabeller vil skjema være mangelfullt. Fikse som for ddl?
    # TODO: Test å kopiere til oracle med constraints -> ikke det i ddl nå hvorfor? -> se også https://sqlite-utils.datasette.io/en/stable/python-api.html#adding-foreign-key-constraints
    # TODO: Fjerne heller enn å telle med nul characters i sqlite? https://www.sqlite.org/nulinstr.html
    # TODO: Oppdater install script så henter ned og bruke ojdbc11.jar heller enn 10
    # TODO: Sikkert at copy statements er i riktig rekkefølge nå?
    # TODO: comment med jdbc type for alle bare? Maxlength for alle datatyper som kan ha det også? https://github.com/frictionlessdata/specs/tree/master/table-schema
    # TODO: Må legge inn fiks på tabell og kolonnenavn som inneholder illegal terms -> må gjøres mot tabell-liste, sqlite og json før lager ddl -> også at ikke over 30
    # --> se her for liste: https://github.com/genomicsengland/ddl-genie/blob/master/ddlgenerator/reserved.py
    # --> For maks 30: select mot original db bruker opprinnelige tabell/kolonnenavn
    # WAIT: Legg inn bruk av args.log_file div steder
    # TODO: Legg inn arg for at target skjema slettes først
    # TODO: Legg inn støtte for brukernavn/passord i h2 url og annet skjema enn std PUBLIC -> fiks også navn på prosjektmappe som blir generert. Også h2 som server og ikke fil!
    # TODO: Se om fornuftige justeringer av ddl-kode her: https://github.com/ezwelty/frictionless-py/blob/7cc51e5a62252c39c83e6b89453fb9a792e644a7/frictionless/plugins/sql.py
    # TODO: Generere ddl med alchemy uten constraints eller de disabled først? -> Table enten til en og samme modell eller printes ut uten sjekker
    # Må gjøre compile for alle tabeller i ett for å unngå FM -> alle må inn i en gruppe med Table objekter. Er det Schema eller model?
    # -> sjekk om noe som kan brukes her: https://github.com/kvesteri/sqlalchemy-utils
    # se sqlalchemy.schema.sort_tables mm her: https://docs.sqlalchemy.org/en/14/core/ddl.html#customizing-ddl
    # TODO: Legg inn at lager ddl for flere dialekter->gjør som her med mock engines: https://github.com/genomicsengland/ddl-genie/blob/master/ddlgenerator/ddlgenerator.py
    # TODO: Generer i content-mappe/{schema1} iso-sql-ddl.sql, mssql-ddl.sql, oracle-ddl.sql mm
    # TODO: Test contextlib.redirect_stderr og hente verdi heller for batch.runScript
    # TODO: Fjern div i sqlwb.py som ikke trengs lenger -> gjør mer testing først!!!
    # TODO: Legg inn test arg som bl.a. sletter tabeller etterhvert som ikke trengs for constraint når oracle eller mssql express og bare test av import
    # TODO: Fjerne lxml igjen? -> test først med å bruke import xml.etree.ElementTree as ET
    # TODO: Må legger inn upper på tabellnavn i get_include_tables og sjekke at fortsatt unike samt oppdatere referanser ift foreign keys, index
    # --> må gjøre tilsvarende også for column names!
    # TODO: Fiks så sqlite Jdbc lik til oracle så henter ut navn/pw mm -> test så til/fra pg/sqlite og restart pc og så Oracle fra/til sqlite
    # TODO: Hvis source er sqlite må en først kjøre jobb som fjerner alle null bytes i tekst felt -> helst i select
    # --> https://www.sqlite.org/nulinstr.html
    # TODO: Bytt ut oracle installasjon i PWLinux med vagrant: https://mikesmithers.wordpress.com/2022/05/08/pretty-vagrant-the-easiest-way-to-get-oracle-xe21c-running-on-ubuntu/
    # TODO: Må legge inn at tabellnavn mm på lowercase når postgresql er target?
    # TODO: Test å lage pakke auto med frictionless: https://framework.frictionlessdata.io/docs/formats/sql.html?query=sql
    # --> se også her: https://raniere-phd.gitlab.io/frictionless-data-handbook/create-tabular-data-package-with-python.html
    # TODO: Legg inn støtte for flere skjemaer
    # TODO: Legg inn at støtter vagrant adresse først i jdbc url i config så starter opp den først automatisk -> må kjøre "vagrant up [id]"
    # TODO: Skriv xml til dbml heller enn omskrevet xml heller
    # TODO: Sjekk hvilken imports som ikke trenger pr py-fil
    # TODO: Legg inn args for å opprette db alias i yaml-fil?->bare et config valg heller hvor dokumenter i fil at kan legge inn alias?
    # ->  Legg også inn args for å teste db connection?
    # TODO: Kan alltid ha user og password som del av jdbc-url slik at kan kutte separate args for de? For både source og target!
    # TODO: Lag funksjon som sjekker om fil allerede finnes mm for å redusere duplisering av kode
    # TODO: Betyr PRAGMA foreign_keys=OFF at en kan ha foreign keys i DDL og så slå på foreign keys igjen etter at lastet data?
    # Test med postgresql og andre som trenger skjema når ikke angitt i url
    # TODO: Legg inn støtte for lesing av aliases->legg til i beskrivelse under --help også da
    # TODO: Legg inn kode for å hente ut mappestruktur
    # Legg inn index på tabeller i sqlite som del av normalisering etter selve uttrekket -> se først normalize_sqlite_db.py
    # WAIT: Legg inn config-valg på om tomme tabeller ikke skal med

    # TODO: Sjekk om kan gjøre kompatibel med e-ark sip/aip (trenger ikke siard likevel):
    # https://github.com/E-ARK-Software/eatb
    # https://github.com/E-ARK-Software/py-e-ark-ip-validator

    # WAIT: Legg inn sjekk på illegal terms pr dbtype->de under ulovlig men mangler info om for hvilken database
    # window transaction function stored schema system notnull column percent date public over sql range member interval start

    # WAIT: Legg til arg "save" som lagrer hele kommando og med annet arg for å hente ut->eller kombinere med alias?

    # -> blir som fra ca linje 254 her https://github.com/rene-bakker-it/lwetl/blob/master/lwetl/programs/db_copy/main.py
    # Bruk egen kode fram til det for å hente metadata/DDL mm!!!


if __name__ == "__main__":
    run(sys.argv)
