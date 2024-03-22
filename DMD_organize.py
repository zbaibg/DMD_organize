import pandas as pd
import os 
from DMDana.lib.DMDparser import *
from DMDana.lib.constant import *
from DMDana.do.config import DMDana_ini_Class
from DMDana.do import *
import logging
from pathlib import Path
from multiprocessing import Pool
root_path=os.getcwd()

def read_database(path):
    return pd.read_excel(path)
def save_database(path,df: pd.DataFrame):
    df.to_excel(path,index=False)
class DMD_organize_class(object):
    def __init__(self,):
        self.df_file_path_in='database_in.xlsx'
        assert os.path.isfile(self.df_file_path_in)
        self.df_file_path_out='database_out.xlsx'
        self.df=read_database(self.df_file_path_in)
        #self.df['id']=range(len(self.df.folders))
    def do(self):
        with Pool() as p:
            p.map(parallelfunc,enumerate(self.df.folders))
        for i,folder in enumerate(self.df.folders):
            tmp_df=read_database('./%d/database_out_%d.xlsx'%(i,i))
            self.df.loc[i,list(tmp_df)]=list(tmp_df.loc[0])
        save_database(self.df_file_path_out,self.df)


def parallelfunc(paramlist):
    i,folder=paramlist
    check_and_create_folder(i)
    folder_analysis(folder,i)

def check_and_create_folder(number):
    if not os.path.isdir("%d"%number):
        os.mkdir("%d"%number)

class folder_analysis(object):
    def __init__(self,folder_path,folder_number):
        self.folder_path=folder_path
        self.folder_number=folder_number
        self.df_one_folder=pd.DataFrame()
        self.log_init()
        logging.info('Start')
        self.df_one_folder.loc[self.folder_number,'folders']=self.folder_path
        #self.df_one_folder.loc[self.folder_number,'id']=self.folder_number
        try:
            self.get_save_param_in()
        except Exception as e:
            self.Find_Error_and_Save(e,"Fail to resolve param.in at folder %d"%self.folder_number)
            return
        try:
            self.get_save_energy()
        except Exception as e:
            self.Find_Error_and_Save(e,"Fail to resolve different energy values at folder %d"%self.folder_number)
            return
        try:
            self.setup_DMDana_ini()
        except Exception as e:
            self.Find_Error_and_Save(e,"Fail to setup DMDana_ini module for folder %d"%self.folder_number)
            return
        try:
            self.DMDana_analysis()
        except Exception as e:
            self.Find_Error_and_Save(e,'DMDana analysis failed at folder %d'%self.folder_number)
            return
        try:
            self.get_DMD_init_folder()
            self.get_kpoint_number()
        except Exception as e:
            self.Find_Error_and_Save(e,'Fail to get DMD_init_folder and kpoint_number at folder %d'%self.folder_number)
            return
        self.Success_and_Save()
        return
    
    def Success_and_Save(self):
        self.df_one_folder.loc[self.folder_number,'organize_status']='Success'
        logging.info("Successfully finished folder %d"%self.folder_number)
        save_database('./%d/database_out_%d.xlsx'%(self.folder_number,self.folder_number),self.df_one_folder)
        return

    def DMDana_analysis(self):
        os.chdir(os.path.join(root_path,"%d"%self.folder_number))
        current_plot.do(self.DMDana_ini)
        FFT_spectrum_plot.do(self.DMDana_ini)
        FFT_DC_convergence_test.do(self.DMDana_ini)
        occup_time.do(self.DMDana_ini)
        occup_deriv.do(self.DMDana_ini)
        os.chdir(root_path)

    def log_init(self):
        os.chdir(root_path)
        logging.basicConfig(
            level=logging.DEBUG,
            filename='./%d/folder_%d.log'%(self.folder_number,self.folder_number),
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p',
            filemode='w',force=True)

    def setup_DMDana_ini(self):
        os.chdir(root_path)
        logging.info('Start to set up DMDana_module for this folder, fake log start to be written')
        self.DMDana_ini=DMDana_ini_Class()
        self.DMDana_ini.folderlist=[self.folder_path]
        self.total_step_number=get_total_step_number(self.folder_path)
        self.DMDana_ini.DMDana_ini_configparser['FFT-spectrum-plot']['Cutoff_list']=str(np.max([self.total_step_number-1000,1]))
        filelist_step=int(np.round(500/self.DMDana_ini.get_folder_config('occup_time',0).occup_timestep_for_all_files))
        self.DMDana_ini.DMDana_ini_configparser['occup-time']['filelist_step']=str(filelist_step)
        self.DMDana_ini.DMDana_ini_configparser['occup-time']['t_max']=str(int(np.floor(np.min([self.DMDana_ini.get_folder_config('occup_time',0).occup_t_tot,2000]))))
        logging.info('Finish seting DMDana_module for this folder, fake log finished')

    def Find_Error_and_Save(self, exception,error_message):
        os.chdir(root_path)
        logging.error(error_message)
        logging.exception(exception)
        self.df_one_folder.loc[self.folder_number,'organize_status']='Fail'
        save_database('./%d/database_out_%d.xlsx'%(self.folder_number,self.folder_number),self.df_one_folder)
        return

    def get_save_param_in(self):
        os.chdir(root_path)
        self.DMDparam_value=get_DMD_param(self.folder_path)
        self.df_one_folder.loc[self.folder_number,list(self.DMDparam_value)]=list(self.DMDparam_value.values())

    def get_save_energy(self):
        os.chdir(root_path)
        self.EBot_probe_au, self.ETop_probe_au, self.EBot_dm_au, self.ETop_dm_au,\
        self.EBot_eph_au, self.ETop_eph_au ,self.EvMax_au, self.EcMin_au=\
        get_erange(self.folder_path)
        self.df_one_folder.loc[self.folder_number,\
        ["EBot_probe_au", "ETop_probe_au", "EBot_dm_au", "ETop_dm_au",\
        "EBot_eph_au", "ETop_eph_au" ,"EvMax_au", "EcMin_au"]]=\
        [self.EBot_probe_au/eV, self.ETop_probe_au/eV, self.EBot_dm_au/eV, self.ETop_dm_au/eV,\
        self.EBot_eph_au/eV, self.ETop_eph_au/eV ,self.EvMax_au/eV, self.EcMin_au/eV]

        self.mu_au,self.temperature_au=get_mu_temperature(self.DMDparam_value,self.folder_path)
        self.df_one_folder.loc[self.folder_number,\
        ["mu_eV","temperature_K"]]=[self.mu_au/eV,self.temperature_au/Kelvin]
    def get_DMD_init_folder(self):
        os.chdir(root_path)
        ldbd_data_path=os.path.abspath('ldbd_data')
        while(os.path.islink(ldbd_data_path)):
            ldbd_data_path=os.readlink(self.folder_path+'/ldbd_data')
        self.DMD_init_folder=os.path.dirname(ldbd_data_path)
        self.df_one_folder.loc[self.folder_number,'DMD_init_folder']=self.DMD_init_folder
        return
    def get_kpoint_number(self):
        self.Full_k_mesh=read_text_from_file(self.DMD_init_folder+'/lindbladInit.out',marlist=['Effective interpolated k-mesh dimensions']*3,locationlist=[5,6,7],stop_at_first_find=True)
        self.DFT_k_fold=read_text_from_file(self.DMD_init_folder+'/lindbladInit.out',marlist=['kfold =']*3,locationlist=[3,4,5],stop_at_first_find=True)
        self.k_number=read_text_from_file(self.DMD_init_folder+'/lindbladInit.out',marlist=['k-points with active states from'],locationlist=[2],stop_at_first_find=True)
        self.df_one_folder.loc[self.folder_number,'Full_k_mesh']=self.Full_k_mesh
        self.df_one_folder.loc[self.folder_number,'DFT_k_fold']=self.DFT_k_fold
        self.df_one_folder.loc[self.folder_number,'k_number']=self.k_number
        return
if __name__=="__main__":
    DMD_organize=DMD_organize_class()
    DMD_organize.do()