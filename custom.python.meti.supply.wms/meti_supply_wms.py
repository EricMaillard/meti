import logging
from datetime import datetime as dt
from ruxit.api.snapshot import pgi_name
from ruxit.api.base_plugin import BasePlugin
from ruxit.api.selectors import FromPluginSelector
from ruxit.api.data import PluginMeasurement
import re
import os
import fnmatch
import json
import requests

logger = logging.getLogger(__name__)

oneagent_log_ingest_url = "http://127.0.0.1:14499/v2/logs/ingest"
dt_header = {
            'Content-Type': 'application/json; charset=utf-8'
}

class Commande:
    def __init__(self, num_cde, nb_articles, site, application, dossier, num_magasin, supply, cde_file_path, transfert_ok, no_traitement, nr_preparation, date_fin_transfert):
        self.num_cde = num_cde
        self.nb_articles = nb_articles
        self.site = site
        self.application = application
        self.dossier = dossier
        self.num_magasin = num_magasin
        self.supply = supply
        self.cde_file_path = cde_file_path
        self.transfert_ok = transfert_ok
        self.date_fin_transfert = date_fin_transfert
        self.no_traitement = no_traitement
        self.nr_preparation = nr_preparation

    
    def getLogEvent(self):
        if self.transfert_ok:
            self.status = "ENVOYE_VERS_WMS"
        else:
            self.status = "ERREUR_ENVOI_VERS_WMS"

        logevent = { 
            "Site" : self.site,
            "Application" : self.application,
            "Dossier" : self.dossier,
            "Numero_Magasin" : self.num_magasin,
            "Numero_Supply" : self.supply,
            "Numero de traitement" : self.no_traitement,
            "Numero_Preparation" : self.nr_preparation ,
            "Nom_Fichier" :self.cde_file_path,
            "Numero_Commande" : self.num_cde,
            "Nombre_Articles" : self.nb_articles,
            "Status" : self.status,
            "Date_Envoi" :self.date_fin_transfert
        }
        return logevent

        
    def getStatus(self):
        return self.status
    

class MetiSupplyWms(BasePlugin):
    def initialize(self, **kwargs):

        # note : this parameter is not used yet in the implementation.
        application = self.config["application"]

        # /meti/emag/log/envpel.SFTP.[Nr Supply].C2475_WMS04RO.*.log 
        # /meti/emag/log/envpel.SFTP.PRPLE1.C2475_WMS04RO.558.20230323_103842.log"
        self.log_file_directory = "/meti/"+application+"/log/"
        logger.info("log_file_directory = "+self.log_file_directory)
        self.log_file_pattern = "envpel.SFTP.*.C2475_WMS04RO.*.log"
        logger.info("log_file_pattern = "+self.log_file_pattern)

        debugLogging = self.config["debug"]
        if debugLogging:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.WARNING)

    def query(self, **kwargs):
        now = dt.now()
        #if self.counter % 60 != 0:
        logger.info("query : "+str(now.minute))
        if now.minute != 5 and now.minute != 20 and now.minute != 35 and now.minute != 50:
            return

        logger.info("Looking at new log files in "+self.log_file_directory)
        liste_commandes = []

        log_files_to_read = []
        for item in os.scandir(self.log_file_directory):
            if item.is_file():
                filename = item.name
                if fnmatch.fnmatch(filename, self.log_file_pattern):
                    modification_time = item.stat().st_mtime
                    modification_date = dt.fromtimestamp(modification_time)
                    delta = now - modification_date
                    if delta.total_seconds() < 15*60:
                        log_files_to_read.append(item.path)
        
        if len(log_files_to_read) == 0:
            logger.info("no new log file")
        # for each log file created in last 15 minutes in the directory, we extract data
        for log_file in log_files_to_read:
            with open(log_file, mode="r",encoding="cp1252") as fp:
                lines = fp.readlines()

            site = "N/A"
            application = "N/A"
            dossier = "N/A"
            fichier = "N/A"
            file_path = "N/A"
            supply = "N/A"
            no_traitement = "N/A"
            transfert_ok = False
            date_fin_transfert = "N/A"

            dossier_found = False
            fichier_found = False
            chemin_found = False

            for line in lines:
                if not dossier_found and 'Dossier            :' in line:
                    reg = 'Dossier            : (.*?)$'
                    search_result = re.search(reg, line)
                    dossier = search_result.group(1)
                    dossier_found = True
                if not fichier_found and 'Nom                :' in line:
                    reg = 'Nom                : (.*?)$'
                    search_result = re.search(reg, line)
                    fichier = search_result.group(1)
                    fichier_found = True
                if not chemin_found and 'Chemin             :' in line:
                    reg = 'Chemin             : (.*?)$'
                    search_result = re.search(reg, line)
                    file_path = search_result.group(1)
                    chemin_found = True
                if 'Send notification to FGO' in line:
                    transfert_ok = True
                if 'Execution trace' in line:
                    reg = '\[INFO\] (.*?) - Execution trace'
                    search_result = re.search(reg, line)
                    if search_result:
                        date_fin_transfert = search_result.group(1)

            supply = dossier
            application = file_path.split("/")[2]
            site = file_path.split("/")[4]
            no_traitement = fichier.split(".")[-1]
            nr_preparation = fichier.split("_")[0]


            file = file_path+"/"+fichier
            if os.path.isfile(file):
                with open(file, mode="r",encoding="utf-8") as fp:
                    lines = fp.readlines()

                num_cde = ""
                nb_articles = 0
                num_magasin = ""

                for line in lines:
                    if line.startswith("PFA"):
                        if nb_articles > 0:
                            commande = Commande(num_cde, nb_articles, site, application, dossier, num_magasin, supply, fichier, transfert_ok, no_traitement, nr_preparation, date_fin_transfert)
                            liste_commandes.append(commande)
                        nb_articles = 0
                        num_cde = line.split("|")[2]
                    if line.startswith("CLI"):
                        num_magasin = line.split("|")[1]
                    if line.startswith("DFA"):
                        nb_articles += 1


        if len(log_files_to_read) == 0:
            logger.info("No log file found")
            return
        if len(liste_commandes) == 0:
            logger.info("No commands")
            return
        log_json = []
        for commande in liste_commandes:
            log_payload = {}
            log_payload['content'] = json.dumps(commande.getLogEvent())
            log_payload['log.step_name'] = 'commander.meti_supply_envoi_wms'
            log_payload['log.source'] = 'flux_commander'
            if commande.getStatus() == "ENVOYE_VERS_WMS":
                log_payload['severity'] = 'info'
            else:
                log_payload['severity'] = 'error'
            log_json.append(log_payload)
        self.sendLogEvents(log_json)
        
    def sendLogEvents(self, log_json):
        bucket_size = 1000

        nb_lines = len(log_json)
        if nb_lines == 0:
            return
    
        number_of_iterations = nb_lines // bucket_size
        logger.info("number_of_iterations = "+str(number_of_iterations))
        last_iteration_len = nb_lines % bucket_size
        for i in range (0, number_of_iterations):
            logger.info('Sending logs from '+str(i*bucket_size)+' to '+str((i*bucket_size)+bucket_size))
            to_send = log_json[i*bucket_size : (i*bucket_size)+bucket_size]
            dynatrace_response = requests.post(oneagent_log_ingest_url, json=to_send, headers=dt_header)
            if dynatrace_response.status_code >= 400:
                jsonContent = json.loads(dynatrace_response.text)
                logger.error(jsonContent)
                logger.error(to_send)
                logger.error(f'Error in Dynatrace log API Response :\n'
                            f'{dynatrace_response}\n'
                            )
        # getting lastest values
        if last_iteration_len > 0:
            logger.info('Sending logs from '+str(number_of_iterations*bucket_size)+' to '+str(number_of_iterations*bucket_size+last_iteration_len))
            to_send = log_json[number_of_iterations*bucket_size : number_of_iterations*bucket_size+last_iteration_len]
            dynatrace_response = requests.post(oneagent_log_ingest_url, json=to_send, headers=dt_header)
            if dynatrace_response.status_code >= 400:
                jsonContent = json.loads(dynatrace_response.text)
                logger.error(jsonContent)
                logger.error(to_send)
                logger.error(f'Error in Dynatrace log API Response :\n'
                            f'{dynatrace_response}\n'
                            )




