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
    if not os.path.isdir(root_path+"/%d"%number):
        os.mkdir(root_path+"/%d"%number)

class folder_analysis(object):
    def __init__(self,folder_path,folder_number):
        self.folder_path=folder_path
        self.folder_number=folder_number
        self.df_one_folder=pd.DataFrame()
        self.log_init()
        self.fail=False
        self.analysis_folder=os.path.join(root_path,"%d"%self.folder_number)
        logging.info('Start')
        self.df_one_folder.loc[self.folder_number,'folders']=self.folder_path
        #self.df_one_folder.loc[self.folder_number,'id']=self.folder_number
        try:
            self.get_save_param_in()
        except Exception as e:
            self.Find_Error_and_Save(e,"Fail to resolve param.in at folder  %d. This is critical, so the script stops."%self.folder_number)
            return
        try:
            self.setup_DMDana_ini()
        except Exception as e:
            self.Find_Error_and_Save(e,"Fail to setup DMDana_ini module for folder  %d. This is critical, so the script stops."%self.folder_number)
            return
        try:
            self.get_save_energy()
        except Exception as e:
            self.Find_Error_and_Save(e,"Fail to resolve different energy values at folder %d"%self.folder_number)
        try:
            self.get_save_mu_T()
        except Exception as e:
            self.Find_Error_and_Save(e,"Fail to get mu and T at folder %d"%self.folder_number)
        try:
            self.current_plot()
        except Exception as e:
            self.Find_Error_and_Save(e,'Fail to do current_plot at folder %d'%self.folder_number)
        try:
            self.FFT_spectrum_plot()
        except Exception as e:
            self.Find_Error_and_Save(e,'Fail to do FFT_spectrum_plot at folder %d'%self.folder_number)
        try:
            self.FFT_DC_convergence_test()
        except Exception as e:
            self.Find_Error_and_Save(e,'Fail to do FFT_DC_convergence_test at folder %d'%self.folder_number)
        try:
            self.occup_time()
        except Exception as e:
            self.Find_Error_and_Save(e,'Fail to do occup_time at folder %d'%self.folder_number)
        try:
            self.occup_deriv()
        except Exception as e:
            self.Find_Error_and_Save(e,'Fail to do occup_deriv at folder %d'%self.folder_number)
        try:
            self.get_DMD_init_folder()
            self.get_kpoint_number()
        except Exception as e:
            self.Find_Error_and_Save(e,'Fail to get DMD_init_folder and kpoint_number at folder %d'%self.folder_number)
        try:
            self.occup_time_short_range_for_better_fit()
        except Exception as e:
            self.Find_Error_and_Save(e,'Fail to do occup_time of short time range for better fitting at folder %d'%self.folder_number)
        self.Determine_Success_and_Save()
        return

    def get_save_mu_T(self):
        self.mu_au,self.temperature_au=get_mu_temperature(self.DMDparam_value,self.folder_path)
        self.df_one_folder.loc[self.folder_number,\
            ["mu_eV","temperature_K"]]=[self.mu_au/eV,self.temperature_au/Kelvin]

    def occup_deriv(self):
        os.chdir(self.analysis_folder)
        occup_deriv.do(self.DMDana_ini)
        os.chdir(root_path)

    def current_plot(self):
        os.chdir(self.analysis_folder)
        current_plot.do(self.DMDana_ini)
        os.chdir(root_path)

    def FFT_spectrum_plot(self):
        os.chdir(self.analysis_folder)
        self.total_step_number=get_total_step_number(self.folder_path)
        self.df_one_folder.loc[self.folder_number,'step_number_finished']=self.total_step_number
        self.DMDana_ini.set('FFT-spectrum-plot','Cutoff_list',max(self.total_step_number-1000,1))
        FFT_spectrum_plot.do(self.DMDana_ini)
        os.chdir(root_path)

    def FFT_DC_convergence_test(self):
        os.chdir(self.analysis_folder)
        FFT_DC_convergence_test.do(self.DMDana_ini)
        os.chdir(root_path)

    def occup_time(self):
        os.chdir(self.analysis_folder)
        occup_time_config_tmp=self.DMDana_ini.get_folder_config('occup_time',0,show_init_log=False)
        occup_timestep_for_all_files=occup_time_config_tmp.occup_timestep_for_all_files
        filelist_step=int(np.round(250/occup_timestep_for_all_files))
        self.DMDana_ini.set('occup-time','filelist_step',filelist_step)
        occup_t_tot=occup_time_config_tmp.occup_t_tot
        t_max_for_occup_time=-1 if occup_t_tot<1302 else 1302
        self.DMDana_ini.set('occup-time','t_max',t_max_for_occup_time)
        occup_time.do(self.DMDana_ini)
        os.chdir(root_path)
    
    def Determine_Success_and_Save(self):
        if not self.fail:
            self.df_one_folder.loc[self.folder_number,'organize_status']='Successed'
            logging.info("Successfully finished folder %d"%self.folder_number)
        else:
            self.df_one_folder.loc[self.folder_number,'organize_status']='Failed but ran to the end'
            logging.info("Parts of the script report error, but it runs until the end. (Folder %d)"%self.folder_number)
        save_database('./%d/database_out_%d.xlsx'%(self.folder_number,self.folder_number),self.df_one_folder)
        return

    def occup_time_short_range_for_better_fit(self):
        occup_time_config_tmp=self.DMDana_ini.get_folder_config('occup_time',0,show_init_log=False)
        occup_Emax_au=occup_time_config_tmp.occup_Emax_au
        EcMin_au=energy_class(self.folder_path).EcMin_au
        self.DMDana_ini.set('occup-time','plot_conduction_valence',False)
        self.DMDana_ini.set('occup-time','occup_time_plot_set_Erange',True)
        self.DMDana_ini.set('occup-time','occup_time_plot_lowE',(occup_Emax_au/eV+EcMin_au/eV)/2)
        self.DMDana_ini.set('occup-time','occup_time_plot_highE',occup_Emax_au/eV)
        os.chdir(self.analysis_folder)
        occup_time.do(self.DMDana_ini)
        os.chdir(root_path)

    def log_init(self):
        logging.basicConfig(
            level=logging.INFO,
            filename=root_path+'/%d/folder_%d.log'%(self.folder_number,self.folder_number),
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p',
            filemode='w',force=True)

    def setup_DMDana_ini(self):
        #logging.info('Start to set up DMDana_module for this folder, possible fake logs may start to be written until the ending is notified.')
        self.DMDana_ini=DMDana_ini_Class(param_path=root_path+"/DMDana.ini")
        self.DMDana_ini.folderlist=[self.folder_path]
        #logging.info('Finish seting DMDana_module for this folder, fake log finished')

    def Find_Error_and_Save(self, exception,error_message):
        os.chdir(root_path)
        logging.error(error_message)
        logging.exception(exception)
        self.fail=True
        self.df_one_folder.loc[self.folder_number,'organize_status']='Fail'
        save_database('./%d/database_out_%d.xlsx'%(self.folder_number,self.folder_number),self.df_one_folder)
        return

    def get_save_param_in(self):
        self.DMDparam_value=get_DMD_param(self.folder_path)
        self.df_one_folder.loc[self.folder_number,list(self.DMDparam_value)]=list(self.DMDparam_value.values())

    def get_save_energy(self):
        energy=energy_class(self.folder_path)
        self.df_one_folder.loc[self.folder_number,\
        ["EBot_probe_eV", "ETop_probe_eV", "EBot_dm_eV", "ETop_dm_eV",\
        "EBot_eph_eV", "ETop_eph_eV" ,"EvMax_eV", "EcMin_eV"]]=\
        [energy.EBot_probe_au/eV, energy.ETop_probe_au/eV, energy.EBot_dm_au/eV, energy.ETop_dm_au/eV,\
        energy.EBot_eph_au/eV, energy.ETop_eph_au/eV ,energy.EvMax_au/eV, energy.EcMin_au/eV]

    def get_DMD_init_folder(self):
        os.chdir(self.folder_path+'/ldbd_data')
        ldbd_data_path=os.getcwd()
        self.DMD_init_folder=os.path.dirname(ldbd_data_path)
        self.df_one_folder.loc[self.folder_number,'DMD_init_folder']=self.DMD_init_folder
        os.chdir(root_path)
        return
    def get_kpoint_number(self):
        self.Full_k_mesh=read_text_from_file(self.DMD_init_folder+'/lindbladInit.out',marklist=['Effective interpolated k-mesh dimensions']*3,locationlist=[5,6,7],stop_at_first_find=True)
        self.DFT_k_fold=read_text_from_file(self.DMD_init_folder+'/lindbladInit.out',marklist=['kfold =']*3,locationlist=[3,4,5],stop_at_first_find=True)
        self.k_number=read_text_from_file(self.DMD_init_folder+'/lindbladInit.out',marklist=['k-points with active states from'],locationlist=[1],stop_at_first_find=True)[0]
        self.DFT_k_fold=[int(i) for i in self.DFT_k_fold]
        self.Full_k_mesh=[int(i) for i in self.Full_k_mesh]
        self.df_one_folder.loc[self.folder_number,'Full_k_mesh']=str(self.Full_k_mesh)
        self.df_one_folder.loc[self.folder_number,'DFT_k_fold']=str(self.DFT_k_fold)
        self.df_one_folder.loc[self.folder_number,'k_number']=self.k_number
        return
class energy_class(object):
    def __init__(self,folder) -> None:
        self.EBot_probe_au, self.ETop_probe_au, self.EBot_dm_au, self.ETop_dm_au,\
        self.EBot_eph_au, self.ETop_eph_au ,self.EvMax_au, self.EcMin_au=\
        get_erange(folder)
if __name__=="__main__":
    DMD_organize=DMD_organize_class()
    DMD_organize.do()