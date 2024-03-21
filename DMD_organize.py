import pandas as pd
import os 
from DMDana.lib.DMDparser import *
from DMDana.lib.constant import *
from DMDana.do.config import DMDana_ini_Class
from DMDana.do import *
import logging
root_path=os.getcwd()
class DMD_organize(object):
    def __init__(self,):
        self.df_file_path_in='database_in.xlsx'
        assert os.path.isfile(self.df_file_path_in)
        self.df_file_path_out='database_out.xlsx'
        self.df = None
        
        self.read_database(self.df_file_path_in)
        #self.DMDana_ini=DMDana_ini_Class('./DMDana.ini')
    def read_database(self,path):
        self.df=pd.read_excel(path)
    def save_database(self,path):
        self.df.to_excel(path,index=False)
    def do(self):
        for i,folder in enumerate(self.df.folders):
            assert os.path.isdir(folder)
            f=folder_analysis(folder,i)
            self.df.loc[i,list(f.DMDparam_value)]=list(f.DMDparam_value.values())
            self.df.loc[i,\
            ["EBot_probe_au", "ETop_probe_au", "EBot_dm_au", "ETop_dm_au",\
            "EBot_eph_au", "ETop_eph_au" ,"EvMax_au", "EcMin_au"]]=\
            [f.EBot_probe_au/eV, f.ETop_probe_au/eV, f.EBot_dm_au/eV, f.ETop_dm_au/eV,\
            f.EBot_eph_au/eV, f.ETop_eph_au/eV ,f.EvMax_au/eV, f.EcMin_au/eV]
            self.df.loc[i,\
            ["mu_eV","temperature_K"]]=[f.mu_au/eV,f.temperature_au/Kelvin]
        self.save_database(self.df_file_path_out)
class folder_analysis(object):
    def __init__(self,folder_path,folder_number):
        logging.basicConfig(
            level=logging.INFO,
            filename='folder_%d.log'%folder_number,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p',
            filemode='w',force=True)
        self.folder_path=folder_path 
        self.DMDparam_value=get_DMD_param(self.folder_path)
        self.jx_data,self.jy_data,self.jz_data=get_current_data(self.folder_path)
        self.EBot_probe_au, self.ETop_probe_au, self.EBot_dm_au, self.ETop_dm_au,\
        self.EBot_eph_au, self.ETop_eph_au ,self.EvMax_au, self.EcMin_au=\
        get_erange(self.folder_path)
        self.mu_au,self.temperature_au=get_mu_temperature(self.DMDparam_value,self.folder_path)
        self.DMDana_ini=DMDana_ini_Class()
        self.DMDana_ini.folderlist=[self.folder_path]
        self.total_step_number=get_total_step_number(self.folder_path)
        self.DMDana_ini.DMDana_ini_configparser['FFT-spectrum-plot']['Cutoff_list']=str(np.max([self.total_step_number-1000,1]))
        if not os.path.isdir("%d"%folder_number):
            os.mkdir("%d"%folder_number)
        for _ in[os.chdir(os.path.join(root_path,"%d"%folder_number))]:
            
            current_plot.do(self.DMDana_ini)
            FFT_spectrum_plot.do(self.DMDana_ini)
            FFT_DC_convergence_test.do(self.DMDana_ini)
            occup_time.do(self.DMDana_ini)
            occup_deriv.do(self.DMDana_ini)
        logging.info("Successfully finished folder %d"%folder_number)
        os.chdir(root_path)
if __name__=="__main__":
    tmp=DMD_organize()
    tmp.do()