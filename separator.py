import csv
import logging
import time
import os
from datetime import datetime, timedelta

def process_large_csv(input_file):

    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)

    start_date = datetime.strptime("01/01/2025", "%d/%m/%Y")


    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("Début du traitement du fichier : %s", input_file)


    logging.info("Comptage des lignes...")
    start_time = time.time()
    with open(input_file, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f) - 1  
    duration = time.time() - start_time
    logging.info("Nombre total de lignes (hors en-tête) : %d", total_lines)
    logging.info("Temps pour le comptage : %.2f secondes", duration)

    half = total_lines // 2


    logging.info("Début de la séparation des fichiers...")

    processed = 0
    last_percent = 0

    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)

        output_files = [
            os.path.join(output_dir, (start_date + timedelta(days=i)).strftime("%d-%m-%Y") + ".csv")
            for i in range(3)
        ]

        outs = [open(f, 'w', newline='', encoding='utf-8') for f in output_files]
        writers = [csv.writer(out) for out in outs]
        for writer in writers:
            writer.writerow(header)


        chunk = total_lines // 3
        for i, row in enumerate(reader):
            processed += 1
            if i < chunk:
                writers[0].writerow(row)
            elif i < 2 * chunk:
                writers[1].writerow(row)
            else:
                writers[2].writerow(row)

            percent = int((processed / total_lines) * 100)
            if percent >= last_percent + 10:
                logging.info("Progression : %d%% (%d lignes traitées)", percent, processed)
                last_percent = percent

    for out in outs:
        out.close()

    logging.info("Fichiers générés :")
    for f in output_files:
        logging.info("- %s", f)
    logging.info("Traitement terminé avec succès.")


process_large_csv("US_Accidents_March23.csv")