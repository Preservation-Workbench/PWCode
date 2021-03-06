# Copyright(C) 2021 Morten Eek

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

# from PIL import Image
import os
import subprocess
import shutil
import filetype
# import sys
import signal
import zipfile
import re
import pathlib
# import img2pdf
import petl as etl
import base64
from os.path import relpath
from pathlib import Path
from common.metadata import run_tika, run_siegfried
from common.file import append_tsv_row, append_txt_file
import cchardet as chardet
# from pathlib import Path
# from functools import reduce
# import wand
# from wand.image import Image, Color
# from wand.exceptions import BlobError
if os.name == "posix":
    import ocrmypdf
    from pdfy import Pdfy

# mime_type: (keep_original, function name, new file extension)
mime_to_norm = {
    'application/msword': (False, 'docbuilder2x', 'pdf'),
    'application/pdf': (False, 'pdf2pdfa', 'pdf'),
    'application/rtf': (False, 'abi2x', 'pdf'),
    'application/vnd.ms-excel': (True, 'docbuilder2x', 'pdf'),
    # 'application/vnd.ms-project': ('pdf'), # TODO: Har ikke ferdig kode for denne ennå
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': (True, 'docbuilder2x', 'pdf'),
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': (True, 'docbuilder2x', 'pdf'),
    'application/vnd.openxmlformats-officedocument.presentationml.presentation': (True, 'docbuilder2x', 'pdf'),
    'application/vnd.wordperfect': (False, 'docbuilder2x', 'pdf'),  # TODO: Mulig denne må endres til libreoffice
    # 'application/xhtml+xml; charset=UTF-8': (False, 'wkhtmltopdf', 'pdf'),
    'application/xhtml+xml': (False, 'wkhtmltopdf', 'pdf'),
    # 'application/xml': (False, 'file_copy', 'xml'),
    'application/xml': (False, 'x2utf8', 'xml'),
    # 'application/x-elf': (False, 'what?', None),  # executable on lin
    # 'application/x-msdownload': (False, 'what?', None),  # executable on win
    # 'application/x-ms-installer': (False, 'what?', None),  # Installer on win
    'application/x-tika-msoffice': (False, 'delete_file', None),  # TODO: Skriv funksjon ferdig
    'application/zip': (False, 'extract_nested_zip', 'zip'),  # TODO: Legg inn for denne
    'image/gif': (False, 'image2norm', 'pdf'),
    # 'image/jpeg': (False, 'image2norm', 'pdf'),
    'image/jpeg': (False, 'file_copy', 'jpg'),
    'image/png': (False, 'file_copy', 'png'),
    'image/tiff': (False, 'image2norm', 'pdf'),
    'text/html': (False, 'html2pdf', 'pdf'),  # TODO: Legg til undervarianter her (var opprinnelig 'startswith)
    'text/plain': (False, 'x2utf8', 'txt'),
    # 'message/rfc822': (False, 'eml2pdf', 'pdf'), # TODO: Sjekk om oppdatert 3. party finnes først. Noen tilfeller av korrupt pdf. Encoding problem. Relaterte?
}


# Dictionary of converter functions
converters = {}


def add_converter():
    # Decorator for adding functions to converter functions
    def _add_converter(func):
        converters[func.__name__] = func
        return func
    return _add_converter


# def delete_file():
#     # TODO: Fjern garbage files og oppdater i tsv at det er gjort
#     return


@add_converter()
def eml2pdf(args):
    ok = False

    repls = (
        ('‘', 'æ'),
        ('›', 'ø'),
        ('†', 'å'),
        ('=C2=A0', ' '),
        ('=C3=A6', 'æ'),
        ('=C3=B8|==C3=B8|=C=3=A5|=C3==B8|=C3=B=8', 'ø'),
        ('=C3=A5|==C3=A5|=C=3=A5|=C3==A5|=C3=A=5', 'å'),  # TODO: Virket ikke siden multiline ikke ignorerte line breaks -> skulle den  ikke det?
        # TODO: Fiks tilfeller når hex variant som skal byttes ut er på hvers av = + line break (er = på slutten av linjer i eml)
        # Multiline skal ignorere line break -> bare alltid ignorere = også og sjekk mot eg C2A0 heller enn =C2=A0 ?
    )

    with open(args['norm_file_path'], "wb") as file:  # TODO: Endre til tmp_file_path
        with open(args['source_file_path'], 'rb') as file_r:
            content = file_r.read()
            data = content.decode(chardet.detect(content)['encoding'])
            for k, v in repls:
                data = re.sub(k, v, data, 0, re.MULTILINE)
        file.write(data.encode('latin1'))  # TODO: Test eml-to-pdf-converter igjen senere for om utf-8 støtte på plass. Melde inn som feil?

    if os.path.exists(args['norm_file_path']):  # TODO: Endre til tmp_file_path
        # TODO konverter til pdf mm som norm_file_path
        ok = True

    return ok


@add_converter()
def x2utf8(args):
    # TODO: Sjekk om beholder extension alltid (ikke endre csv, xml mm)
    # TODO: Test å bruke denne heller enn/i tillegg til replace under: https://ftfy.readthedocs.io/en/latest/

    repls = (
        ('‘', 'æ'),
        ('›', 'ø'),
        ('†', 'å'),
        ('&#248;', 'ø'),
        ('&#229;', 'å'),
        ('&#230;', 'æ'),
        ('&#197;', 'Å'),
        ('&#216;', 'Ø'),
        ('&#198;', 'Æ'),
        ('=C2=A0', ' '),
        ('=C3=A6', 'æ'),
        ('=C3=B8', 'ø'),
        ('=C3=A5', 'å'),
    )

    with open(args['norm_file_path'], "wb") as file:
        with open(args['source_file_path'], 'rb') as file_r:
            content = file_r.read()
            if content is None:
                return False

            char_enc = chardet.detect(content)['encoding']

            try:
                data = content.decode(char_enc)
            except Exception:
                return False

            for k, v in repls:
                data = re.sub(k, v, data, flags=re.MULTILINE)
        file.write(data.encode('utf8'))

        if os.path.exists(args['norm_file_path']):
            return True

    return False


def extract_nested_zip(zippedFile, toFolder):
    """ Extract a zip file including any nested zip files
        Delete the zip file(s) after extraction
    """
    # pathlib.Path(toFolder).mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zippedFile, 'r') as zfile:
        zfile.extractall(path=toFolder)
    os.remove(zippedFile)
    for root, dirs, files in os.walk(toFolder):
        for filename in files:
            if re.search(r'\.zip$', filename):
                fileSpec = os.path.join(root, filename)
                extract_nested_zip(fileSpec, root)


def kill(proc_id):
    os.kill(proc_id, signal.SIGINT)


def run_shell_command(command, cwd=None, timeout=30):
    # ok = False
    os.environ['PYTHONUNBUFFERED'] = "1"
    # cmd = [' '.join(command)]
    stdout = []
    stderr = []
    mix = []  # TODO: Fjern denne mm

    # print(''.join(cmd))
    # sys.stdout.flush()

    proc = subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        kill(proc.pid)

    # while proc.poll() is None:
    #     line = proc.stdout.readline()
    #     if line != "":
    #         stdout.append(line)
    #         mix.append(line)
    #         print(line, end='')

    #     line = proc.stderr.readline()
    #     if line != "":
    #         stderr.append(line)
    #         mix.append(line)
    #         print(line, end='')

    for line in proc.stdout:
        stdout.append(line.rstrip())

    for line in proc.stderr:
        stderr.append(line.rstrip())

    # print(stderr)
    return proc.returncode, stdout, stderr, mix


@ add_converter()
def file_copy(args):
    ok = False
    try:
        shutil.copyfile(args['source_file_path'], args['norm_file_path'])
        ok = True
    except Exception as e:
        print(e)
        ok = False
    return ok


# TODO: Hvordan kalle denne med python: tesseract my-image.png nytt filnavn pdf -> må bruke subprocess

@ add_converter()
def image2norm(args):
    ok = False
    args['tmp_file_path'] = args['tmp_file_path'] + '.pdf'
    command = ['convert', args['source_file_path'], args['tmp_file_path']]
    run_shell_command(command)

    if os.path.exists(args['tmp_file_path']):
        ok = pdf2pdfa(args)

        # WAIT: Egen funksjon for sletting av tmp-filer som kalles fra alle def? Er nå under her for å håndtere endret tmp navn + i overordnet convert funkson
        if os.path.isfile(args['tmp_file_path']):
            os.remove(args['tmp_file_path'])

    return ok


@ add_converter()
def docbuilder2x(args):
    ok = False
    docbuilder_file = os.path.join(args['tmp_dir'], 'x2x.docbuilder')

    docbuilder = None
    # WAIT: Tremger ikke if/else under hvis ikke skal ha spesifikk kode pr format
    if args['mime_type'] in (
            'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ):
        docbuilder = [
            'builder.OpenFile("' + args['source_file_path'] + '", "")',
            'builder.SaveFile("pdf", "' + args['tmp_file_path'] + '")',
            'builder.CloseFile();',
        ]
    else:
        docbuilder = [
            'builder.OpenFile("' + args['source_file_path'] + '", "")',
            'builder.SaveFile("pdf", "' + args['tmp_file_path'] + '")',
            'builder.CloseFile();',
        ]

    with open(docbuilder_file, "w+") as file:
        file.write("\n".join(docbuilder))

    command = ['documentbuilder', docbuilder_file]
    run_shell_command(command)

    if os.path.exists(args['tmp_file_path']):
        ok = pdf2pdfa(args)

    return ok


@add_converter()
def wkhtmltopdf(args):
    # WAIT: Trengs sjekk om utf-8 og evt. konvertering først her?
    ok = False
    command = ['wkhtmltopdf', '-O', 'Landscape', args['source_file_path'], args['tmp_file_path']]
    run_shell_command(command)

    if os.path.exists(args['tmp_file_path']):
        ok = pdf2pdfa(args)

    return ok


@add_converter()
def abi2x(args):
    ok = False
    command = ['abiword', '--to=pdf', '--import-extension=rtf', '-o', args['tmp_file_path'], args['source_file_path']]
    run_shell_command(command)

    if os.path.exists(args['tmp_file_path']):
        ok = pdf2pdfa(args)

    return ok


# def libre2x(source_file_path, tmp_file_path, norm_file_path, keep_original, tmp_dir, mime_type):
    # TODO: Endre så bruker collabora online (er installert på laptop). Se notater om curl kommando i joplin
    # command = ["libreoffice", "--convert-to", "pdf", "--outdir", str(filein.parent), str(filein)]
    # run_shell_command(command)


def unoconv2x(file_path, norm_path, format, file_type):
    ok = False
    command = ['unoconv', '-f', format]

    if file_type in (
            'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ):
        if format == 'pdf':
            command.extend([
                '-d', 'spreadsheet', '-P', 'PaperOrientation=landscape',
                '-eSelectPdfVersion=1'
            ])
        elif format == 'html':
            command.extend(
                ['-d', 'spreadsheet', '-P', 'PaperOrientation=landscape'])
    elif file_type in ('application/msword', 'application/rtf'):
        command.extend(['-d', 'document', '-eSelectPdfVersion=1'])

    command.extend(['-o', '"' + norm_path + '"', '"' + file_path + '"'])
    run_shell_command(command)

    if os.path.exists(norm_path):
        ok = True

    return ok


@ add_converter()
def pdf2pdfa(args):
    ok = False

    if args['mime_type'] == 'application/pdf':
        args['tmp_file_path'] = args['source_file_path']

        # WAIT: Legg inn ekstra sjekk her om hva som skal gjøres hvis ocr = True
        if args['version'] in ('1a', '1b', '2a', '2b'):
            file_copy(args)
            if os.path.exists(args['norm_file_path']):
                ok = True

            return ok

    ocrmypdf.configure_logging(-1)
    result = ocrmypdf.ocr(args['tmp_file_path'], args['norm_file_path'], tesseract_timeout=0, progress_bar=False, skip_text=True)
    if str(result) == 'ExitCode.ok':
        ok = True

    return ok


@ add_converter()
def html2pdf(args):
    ok = False
    try:
        p = Pdfy()
        p.html_to_pdf(args['source_file_path'], args['tmp_file_path'])
    except Exception as e:
        print(e)

    if os.path.exists(args['tmp_file_path']):
        ok = pdf2pdfa(args)

    return ok


def file_convert(source_file_path, mime_type, function, target_dir, tmp_dir, norm_file_path=None, norm_ext=None, version=None, ocr=False, keep_original=False):
    source_file_name = os.path.basename(source_file_path)
    base_file_name = os.path.splitext(source_file_name)[0] + '.'
    tmp_file_path = tmp_dir + '/' + base_file_name + 'tmp'
    if norm_file_path is None:
        norm_file_path = target_dir + '/' + base_file_name
        if norm_ext:
            norm_file_path = norm_file_path + norm_ext

    # TODO: Endre så returneres file paths som starter med prosjektmappe? Alltid, eller bare når genereres arkivpakke?
    normalized = {'result': None, 'norm_file_path': norm_file_path, 'error': None, 'original_file_copy': None}

    if not os.path.isfile(norm_file_path):
        if os.path.islink(source_file_path):
            normalized['result'] = 5  # Not a file
            normalized['norm_file_path'] = None
        elif function in converters:
            pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)

            function_args = {'source_file_path': source_file_path,
                             'tmp_file_path': tmp_file_path,
                             'norm_file_path': norm_file_path,
                             'keep_original': keep_original,
                             'tmp_dir': tmp_dir,
                             'mime_type': mime_type,
                             'version': version,
                             #  'ocr': ocr,
                             }

            ok = converters[function](function_args)

            if not ok:
                error_files = target_dir + '/error_documents/'
                pathlib.Path(error_files).mkdir(parents=True, exist_ok=True)
                file_copy_args = {'source_file_path': source_file_path,
                                  'norm_file_path': error_files + os.path.basename(source_file_path)
                                  }
                file_copy(file_copy_args)
                normalized['original_file_copy'] = file_copy_args['norm_file_path']  # TODO: Fjern fil hvis konvertering lykkes når kjørt på nytt
                normalized['result'] = 0  # Conversion failed
                normalized['norm_file_path'] = None
            elif keep_original:
                original_files = target_dir + '/original_documents/'
                pathlib.Path(original_files).mkdir(parents=True, exist_ok=True)
                file_copy_args = {'source_file_path': source_file_path,
                                  'norm_file_path': original_files + os.path.basename(source_file_path)
                                  }
                file_copy(file_copy_args)
                normalized['original_file_copy'] = file_copy_args['norm_file_path']
                normalized['result'] = 1  # Converted successfully
            else:
                normalized['result'] = 1  # Converted successfully
        else:
            if function:
                normalized['result'] = 4
                normalized['error'] = "Missing converter function '" + function + "'"
                normalized['norm_file_path'] = None
            else:
                normalized['result'] = 2  # Conversion not supported
                normalized['norm_file_path'] = None
    else:
        normalized['result'] = 3  # Converted earlier, or manually

    if os.path.isfile(tmp_file_path):
        os.remove(tmp_file_path)

    return normalized


def remove_fields(fields, table):
    for field in fields:
        if field in etl.fieldnames(table):
            table = etl.cutout(table, field)
    return table


def add_fields(fields, table):
    for field in fields:
        if field not in etl.fieldnames(table):
            table = etl.addfield(table, field, None)
    return table


def convert_folder(project_dir, base_source_dir, base_target_dir, tmp_dir, java_path, tika=False, ocr=False, merge=False, tsv_source_path=None, tsv_target_path=None):
    # TODO: Legg inn i gui at kan velge om skal ocr-behandles
    txt_target_path = base_target_dir + '_result.txt'
    json_tmp_dir = base_target_dir + '_tmp'
    converted_now = False
    errors = False

    if tsv_source_path is None:
        tsv_source_path = base_target_dir + '.tsv'
    else:
        txt_target_path = os.path.splitext(tsv_source_path)[1][1:] + '_result.txt'

    if tsv_target_path is None:
        tsv_target_path = base_target_dir + '_processed.tsv'

    Path(base_target_dir).mkdir(parents=True, exist_ok=True)

    # TODO: Viser mime direkte om er pdf/a eller må en sjekke mot ekstra felt i de to under?

    if not os.path.isfile(tsv_source_path):
        if tika:
            run_tika(tsv_source_path, base_source_dir, json_tmp_dir, java_path)
        else:
            run_siegfried(base_source_dir, project_dir, tsv_source_path)

    table = etl.fromtsv(tsv_source_path)
    table = etl.rename(table,
                       {
                           'filename': 'source_file_path',
                           'tika_batch_fs_relative_path': 'source_file_path',
                           'filesize': 'file_size',
                           'mime': 'mime_type',
                           'Content_Type': 'mime_type',
                           'Version': 'version'
                       },
                       strict=False)

    thumbs_table = etl.select(table, lambda rec: Path(rec.source_file_path).name == 'Thumbs.db')
    if etl.nrows(thumbs_table) > 0:
        thumbs_paths = etl.values(thumbs_table, 'source_file_path')
        for path in thumbs_paths:
            if '/' not in path:
                path = os.path.join(base_source_dir, path)
            if os.path.isfile(path):
                os.remove(path)

        table = etl.select(table, lambda rec: Path(rec.source_file_path).name != 'Thumbs.db')

    table = etl.select(table, lambda rec: rec.source_file_path != '')
    row_count = etl.nrows(table)

    # error_documents
    # original_documents
    # TODO: Legg inn at ikke skal telle filer i mapper med de to navnene over
    file_count = sum([len(files) for r, d, files in os.walk(base_source_dir)])
    if row_count == 0:
        print('No files to convert. Exiting.')
        return 'error'
    elif file_count != row_count:
        print("Files listed in '" + tsv_source_path + "' doesn't match files on disk. Exiting.")
        return 'error'
    else:
        print('Converting files..')

    # WAIT: Legg inn sjekk på filstørrelse før og etter konvertering

    append_fields = ('version', 'norm_file_path', 'result', 'original_file_copy', 'id')
    table = add_fields(append_fields, table)

    cut_fields = ('0', '1', 'X_TIKA_EXCEPTION_runtime')
    table = remove_fields(cut_fields, table)

    header = etl.header(table)
    append_tsv_row(tsv_target_path, header)

    # Treat csv (detected from extension only) as plain text:
    table = etl.convert(table, 'mime_type', lambda v, row: 'text/plain' if row.id == 'x-fmt/18' else v, pass_row=True)

    # Update for missing mime types where id is known:
    table = etl.convert(table, 'mime_type', lambda v, row: 'application/xml' if row.id == 'fmt/979' else v, pass_row=True)

    if os.path.isfile(txt_target_path):
        os.remove(txt_target_path)

    data = etl.dicts(table)
    count = 0
    for row in data:
        count += 1
        count_str = ('(' + str(count) + '/' + str(file_count) + '): ')
        source_file_path = row['source_file_path']
        if '/' not in source_file_path:
            source_file_path = os.path.join(base_source_dir, source_file_path)

        mime_type = row['mime_type']
        # TODO: Virker ikke når Tika brukt -> finn hvorfor
        if ';' in mime_type:
            mime_type = mime_type.split(';')[0]

        version = row['version']
        result = None
        old_result = row['result']

        print(count_str + source_file_path + ' (' + mime_type + ')')

        if not mime_type:
            # kind = filetype.guess(source_file_path)
            extension = os.path.splitext(source_file_path)[1][1:].lower()
            if extension == 'xml':
                mime_type = 'application/xml'

        if mime_type not in mime_to_norm.keys():
            # print("|" + mime_type + "|")
            errors = True
            converted_now = True
            result = 'Conversion not supported'
            append_txt_file(txt_target_path, result + ': ' + source_file_path + ' (' + mime_type + ')')
            row['norm_file_path'] = ''
            row['original_file_copy'] = ''
        else:
            keep_original = mime_to_norm[mime_type][0]
            function = mime_to_norm[mime_type][1]

            # Ensure unique file names in dir hierarchy:
            norm_ext = (base64.b32encode(bytes(str(count), encoding='ascii'))).decode('utf8').replace('=', '').lower() + '.' + mime_to_norm[mime_type][2]
            target_dir = os.path.dirname(source_file_path.replace(base_source_dir, base_target_dir))
            normalized = file_convert(source_file_path, mime_type, function, target_dir, tmp_dir, None, norm_ext, version, ocr, keep_original)

            if normalized['result'] == 0:
                errors = True
                result = 'Conversion failed'
                append_txt_file(txt_target_path, result + ': ' + source_file_path + ' (' + mime_type + ')')
            elif normalized['result'] == 1:
                result = 'Converted successfully'
                converted_now = True
            elif normalized['result'] == 2:
                errors = True
                result = 'Conversion not supported'
                append_txt_file(txt_target_path, result + ': ' + source_file_path + ' (' + mime_type + ')')
            elif normalized['result'] == 3:
                if old_result not in ('Converted successfully', 'Manually converted'):
                    result = 'Manually converted'
                    converted_now = True
                else:
                    result = old_result
            elif normalized['result'] == 4:
                converted_now = True
                errors = True
                result = normalized['error']
                append_txt_file(txt_target_path, result + ': ' + source_file_path + ' (' + mime_type + ')')
            elif normalized['result'] == 5:
                result = 'Not a file'

            if normalized['norm_file_path']:
                row['norm_file_path'] = relpath(normalized['norm_file_path'], base_target_dir)

            file_copy_path = normalized['original_file_copy']
            if file_copy_path:
                file_copy_path = relpath(file_copy_path, base_target_dir)
            row['original_file_copy'] = file_copy_path

        row['result'] = result
        append_tsv_row(tsv_target_path, list(row.values()))

    shutil.move(tsv_target_path, tsv_source_path)
    # TODO: Legg inn valg om at hvis merge = true kopieres alle filer til mappe på øverste nivå og så slettes tomme undermapper

    msg = None
    if converted_now:
        if errors:
            msg = "Not all files were converted. See '" + txt_target_path + "' for details."
        else:
            msg = 'All files converted succcessfully.'
    else:
        msg = 'All files converted previously.'

    # print(msg)
    return msg  # TODO: Fiks så bruker denne heller for oppsummering til slutt når flere mapper konvertert
