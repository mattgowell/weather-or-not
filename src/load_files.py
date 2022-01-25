import io
import os
import tarfile
import pandas as pd
import psycopg

filename = os.path.join(os.getcwd(), "data/raw/ghcnd_hcn.tar.gz")

widths = (
    11, 4, 2, 4, 
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1,
    5, 1, 1, 1 
    )

columns = (
    "id", "obs_year", "obs_month", "element",
    "value1", "mflag1", "qflag1", "sflag1",
    "value2", "mflag2", "qflag2", "sflag2",
    "value3", "mflag3", "qflag3", "sflag3",
    "value4", "mflag4", "qflag4", "sflag4",
    "value5", "mflag5", "qflag5", "sflag5",
    "value6", "mflag6", "qflag6", "sflag6",
    "value7", "mflag7", "qflag7", "sflag7",
    "value8", "mflag8", "qflag8", "sflag8",
    "value9", "mflag9", "qflag9", "sflag9",
    "value10", "mflag10", "qflag10", "sflag10",
    "value11", "mflag11", "qflag11", "sflag11",
    "value12", "mflag12", "qflag12", "sflag12",
    "value13", "mflag13", "qflag13", "sflag13",
    "value14", "mflag14", "qflag14", "sflag14",
    "value15", "mflag15", "qflag15", "sflag15",
    "value16", "mflag16", "qflag16", "sflag16",
    "value17", "mflag17", "qflag17", "sflag17",
    "value18", "mflag18", "qflag18", "sflag18",
    "value19", "mflag19", "qflag19", "sflag19",
    "value20", "mflag20", "qflag20", "sflag20",
    "value21", "mflag21", "qflag21", "sflag21",
    "value22", "mflag22", "qflag22", "sflag22",
    "value23", "mflag23", "qflag23", "sflag23",
    "value24", "mflag24", "qflag24", "sflag24",
    "value25", "mflag25", "qflag25", "sflag25",
    "value26", "mflag26", "qflag26", "sflag26",
    "value27", "mflag27", "qflag27", "sflag27",
    "value28", "mflag28", "qflag28", "sflag28",
    "value29", "mflag29", "qflag29", "sflag29",
    "value30", "mflag30", "qflag30", "sflag30",
    "value31", "mflag31", "qflag31", "sflag31"
    )

id_vars = ("id", "obs_year", "obs_month", "element")
value_vars = (
    "value1", "mflag1", "qflag1", "sflag1",
    "value2", "mflag2", "qflag2", "sflag2",
    "value3", "mflag3", "qflag3", "sflag3",
    "value4", "mflag4", "qflag4", "sflag4",
    "value5", "mflag5", "qflag5", "sflag5",
    "value6", "mflag6", "qflag6", "sflag6",
    "value7", "mflag7", "qflag7", "sflag7",
    "value8", "mflag8", "qflag8", "sflag8",
    "value9", "mflag9", "qflag9", "sflag9",
    "value10", "mflag10", "qflag10", "sflag10",
    "value11", "mflag11", "qflag11", "sflag11",
    "value12", "mflag12", "qflag12", "sflag12",
    "value13", "mflag13", "qflag13", "sflag13",
    "value14", "mflag14", "qflag14", "sflag14",
    "value15", "mflag15", "qflag15", "sflag15",
    "value16", "mflag16", "qflag16", "sflag16",
    "value17", "mflag17", "qflag17", "sflag17",
    "value18", "mflag18", "qflag18", "sflag18",
    "value19", "mflag19", "qflag19", "sflag19",
    "value20", "mflag20", "qflag20", "sflag20",
    "value21", "mflag21", "qflag21", "sflag21",
    "value22", "mflag22", "qflag22", "sflag22",
    "value23", "mflag23", "qflag23", "sflag23",
    "value24", "mflag24", "qflag24", "sflag24",
    "value25", "mflag25", "qflag25", "sflag25",
    "value26", "mflag26", "qflag26", "sflag26",
    "value27", "mflag27", "qflag27", "sflag27",
    "value28", "mflag28", "qflag28", "sflag28",
    "value29", "mflag29", "qflag29", "sflag29",
    "value30", "mflag30", "qflag30", "sflag30",
    "value31", "mflag31", "qflag31", "sflag31"
)

pivot_index_columns = ["id", "obs_year", "obs_month", "element", "day"]

def load_weather_tarfile(filename, widths, columns, id_vars, value_vars, pivot_index_columns):
    print(f"loading {filename}")
    total_records = 0
    files_processed = 0
    with tarfile.open(filename, "r:gz") as tar:
        file_count = len(tar.getnames())
        print(f'Total Files in tar {file_count}')
        with psycopg.connect("dbname=weather user=matt") as conn:
            with conn.cursor() as cur:
                for member in tar:
                    if member.name.endswith('.dly'):
                        with cur.copy("COPY ghcnd_all from STDIN WITH CSV") as copy:
                            buffreader = tar.extractfile(member)
                            text = io.TextIOWrapper(buffreader)
                            df = (pd.read_fwf(text,widths=widths, header=None)
                                    .set_axis(columns, axis=1)
                                    .melt(id_vars=id_vars, value_vars=value_vars)
                            )
                            for day in range(1,31):
                                df.loc[df['variable'].str.contains(f'{day}'),'day'] = day
                            df['variable'] = df['variable'].str.replace(r'\d+','', regex=True)
                            
                            drop_index = df.loc[(df['value'] == -9999) | (df['value'].isnull())].index
                            df.info()
                            print(df.duplicated(subset=pivot_index_columns.append('variable')).value_counts())
                            #df = (df.drop(index=drop_index)
                            #        .pivot(index=pivot_index_columns, columns='variable')
                            #        .pipe(lambda pivoted: pivoted.rename(columns={'value':'measure'}))
                            #)
                            #csv = df.to_csv(index=False, header=False)
                            #copy.write(csv)
                            total_records += len(df.index)
                            files_processed += 1
                            percent_processed = 100*files_processed/file_count
                            print(f'Files processed so far {files_processed} - {percent_processed:.1f}%')
            conn.commit()
        print(f'Total records committed {total_records}')

if __name__ == '__main__':
    load_weather_tarfile(filename, widths, columns, id_vars, value_vars, pivot_index_columns)