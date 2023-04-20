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
    def __init__(self, num_cde, nb_articles, site, application, dossier, num_magasin, supply, cde_file_path, transfert_ok, status_3, date_fin_transfert):
        self.num_cde = num_cde
        self.nb_articles = nb_articles
        self.site = site
        self.application = application
        self.dossier = dossier
        self.num_magasin = num_magasin
        self.supply = supply
        self.cde_file_path = cde_file_path
        self.transfert_ok = transfert_ok
        self.status_3 = status_3
        self.date_fin_transfert = date_fin_transfert
        if self.transfert_ok and self.status_3:
            self.status = "ENVOYE_VERS_SUPPLY"
        else:
            self.status = "ERREUR_ENVOI_VERS_SUPPLY"
   
    def getLogEvent(self):
        if self.cde_file_path != "N/A":
            activite_logistique = self.cde_file_path.split('.')[1]
        else:
            activite_logistique = "N/A"


        logevent = { 
            "Site" : self.site,
            "Application" : self.application,
            "Dossier" : self.dossier,
            "Numero_Magasin" : self.num_magasin,
            "Numero_Supply" : self.supply,
            "Activite_logistique" : activite_logistique,
            "Nom_Fichier" :self.cde_file_path,
            "Numero_Commande" : self.num_cde,
            "Nombre_Articles" : self.nb_articles,
            "Status" : self.status,
            "Date_Envoi" :self.date_fin_transfert
        }
        return logevent
    
    def getStatus(self):
        return self.status

class MetiStoreCde(BasePlugin):
    def initialize(self, **kwargs):

        # note : this parameter is not used yet in the implementation.
        application = self.config["application"]
        site = self.config["site"]
        dossier = self.config["dossier"]

        # /meti/emag/log/ESCSPL/PLS003/ESCSPL_PLS003_APRO_ACHA_CDE_ENVOI_1501020.log
        self.log_file_directory = "/meti/"+application+"/log/"+site+"/"+dossier+"/"
        logger.info("log_file_directory = "+self.log_file_directory)
        self.log_file_pattern = site+"_"+dossier+"_APRO_ACHA_CDE_ENVOI_*.log"
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
            cde_file_path = "N/A"
            supply = "N/A"
            transfert_ok = False
            status_3 = False
            date_fin_transfert = "N/A"

            C2480_CDX = False

            for line in lines:
                if "<CDFLUX>C2480_CDX</CDFLUX>" in line:
                    C2480_CDX = True
                    break
            if C2480_CDX == False:
                logger.info("Log file "+log_file+" is not for C2480_CDX")
            if C2480_CDX:
                for line in lines:
                    if "Site:" in line:
                        reg = 'Site: (.*?)$'
                        search_result = re.search(reg, line)
                        if search_result:
                            site = search_result.group(1)
                    if "Application:" in line:
                        reg = 'Application: (.*?)$'
                        search_result = re.search(reg, line)
                        if search_result:
                            application = search_result.group(1)
                    if "Dossier:" in line:
                        reg = 'Dossier: (.*?)$'
                        search_result = re.search(reg, line)
                        if search_result:
                            dossier = search_result.group(1)
                    if "Historisation du fichier vers" in line:
                        reg = 'Historisation du fichier vers (.*?)$'
                        search_result = re.search(reg, line)
                        if search_result:
                            cde_file_path = search_result.group(1)
                    if "Send C2480_CDX to" in line:
                        reg = 'Send C2480_CDX to (.*?) with'
                        search_result = re.search(reg, line)
                        if search_result:
                            supply = search_result.group(1)
                    if "Transfert via envpel.sh OK" in line:
                        transfert_ok = True
                    if "Le traitement passe en statut 3" in line:
                        status_3 = True
                    if "Fin du Transfert flux site" in line:
                        print(line)
                        reg = '\[INFO\] - (.*?) : Fin du Transfert'
                        search_result = re.search(reg, line)
                        if search_result:
                            date_fin_transfert = search_result.group(1)

            logger.info("Site = "+site)
            logger.info("Application = "+application)
            logger.info("Dossier = "+dossier)
            logger.info("cde_file_path = "+cde_file_path)
            logger.info("supply = "+supply)
            logger.info("transfert_ok = "+str(transfert_ok))
            logger.info("status_3 = "+str(status_3))
            logger.info("date_fin_transfert = "+date_fin_transfert)

            with open(cde_file_path, mode="r",encoding="cp1252") as fp:
                lines = fp.readlines()

            E_lines_list = []
            F_lines_list = []
            for line in lines:
                if line.startswith("E"):
                    E_lines_list.append(line)
                if line.startswith("F"):
                    F_lines_list.append(line)

            if len(E_lines_list) != len(F_lines_list):
                logger.error("We should have the same lenght")
            else:
                length = len(E_lines_list)
                for i in range(0,length):
                    num_magasin = E_lines_list[i][1:7]
                    num_cde = E_lines_list[i][8:15]
                    nb_articles = int(F_lines_list[i][1:9])
                    print("num_magasin = "+num_magasin)
                    print("num_cde = "+num_cde)
                    print("nb_articles = "+str(nb_articles))
                    commande = Commande(num_cde, nb_articles, site, application, dossier, num_magasin, supply, cde_file_path, transfert_ok, status_3, date_fin_transfert)
                    liste_commandes.append(commande)

        if len(log_files_to_read) == 0:
            logger.info("No log file found")
            return
        log_json = []
        for commande in liste_commandes:
            log_payload = {}
            log_payload['content'] = json.dumps(commande.getLogEvent())
            log_payload['log.step_name'] = 'commander.meti_store'
            log_payload['log.source'] = 'flux_commander'
            if commande.getStatus() == "ENVOYE_VERS_SUPPLY":
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
