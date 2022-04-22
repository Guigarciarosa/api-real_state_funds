import pyautogui as pya
import time

class automator():
    # create an automator to run the routine scripts to collect and transform data and create the datamart's
    def start_the_cmd():
        #star cmd
        pya.hotkey('win','r')
        time.sleep(2)
        pya.hotkey('del')
        time.sleep(2)
        pya.write('cmd',interval=0.1)
        time.sleep(2)
        pya.hotkey('enter')
    start_the_cmd()

    def activate_env():
        # activate the virtualenv
        time.sleep(3)
        pya.write(r'C:\Users\guilherme.dias\Documents\area_de_teste\test_work\Scripts\activate.bat')
        time.sleep(1)
        pya.hotkey('enter')
    activate_env()

    def start_scripts():
        # initiate the scripts
        time.sleep(3)
        pya.write(r'cd C:\Users\guilherme.dias\Documents\GitHub\api-real_state_funds\py',interval=0.01)
        time.sleep(1)
        pya.hotkey('enter')
        # real_state_rank
        pya.write('python extract_data.py',interval=0.1)
        time.sleep(1)
        pya.hotkey('enter')
        time.sleep(20)
        pya.write('python get_date_from_real_state.py',interval=0.1)
        time.sleep(1)
        pya.hotkey('enter')
        time.sleep(1)

    start_scripts()

automator()