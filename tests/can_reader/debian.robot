*** Settings ***
Resource        ../resources/common.robot
Library         Cumulocity
Library         OperatingSystem

Suite Setup     Set Main Device


*** Test Cases ***
Device should have installed software tedge-can-plugin
    ${deb_version}=    Get Debian Package Version    ${CURDIR}/../data/tedge-can-plugin.deb
    ${installed}=    Device Should Have Installed Software    tedge-can-plugin
    Should Be Equal    ${installed["tedge-can-plugin"]["version"]}    ${deb_version}

Service should be active
    System D Service should be Active    tedge-can-plugin

ReInstall CAN Plugin
    ${deb_version}=    Get Debian Package Version    ${CURDIR}/../data/tedge-can-plugin.deb
    # Uninstall tedge-can-plugin
    ${uninstall_operation}=    Cumulocity.Uninstall Software
    ...    {"name": "tedge-can-plugin", "version": "${deb_version}", "softwareType": "apt"}
    Operation Should Be SUCCESSFUL    ${uninstall_operation}    timeout=60
    Device Should Not Have Installed Software    tedge-can-plugin

    # Ensure smartrest config has no longer 'can' after uninstall
    ${templates}=    Get tedge smartrest config
    Should Be Equal    ${templates}    []    msg=SmartREST Message should be removed

    # Upload binary
    ${binary_url}=    Cumulocity.Create Inventory Binary
    ...    tedge-can-plugin
    ...    apt
    ...    file=${CURDIR}/../data/tedge-can-plugin.deb
    # Install can Plugin
    ${install_operation}=    Cumulocity.Install Software
    ...    {"name": "tedge-can-plugin", "version": "${deb_version}", "softwareType": "apt", "url": "${binary_url}"}
    Operation Should Be SUCCESSFUL    ${install_operation}    timeout=240
    ${installed}=    Device Should Have Installed Software    tedge-can-plugin
    Should Be Equal    ${installed["tedge-can-plugin"]["version"]}    ${deb_version}
    # Restart Plugin
    ${shell_operation}=    Execute Shell Command
    ...    sudo systemctl restart tedge-can-plugin
    ${shell_operation}=    Cumulocity.Operation Should Be SUCCESSFUL    ${shell_operation}    timeout=60
    # Check if plugin is running
    System D Service should be Active    tedge-can-plugin


*** Keywords ***
Get Debian Package Version
    [Arguments]    ${deb_file_path}
    ${version}=    Run    dpkg-deb -f ${deb_file_path} Version
    Log    ${version}
    RETURN    ${version}

System D Service should be Active
    [Arguments]    ${system_d_service}
    ${shell_operation}=    Execute Shell Command
    ...    sudo systemctl status ${system_d_service} | grep Active
    ${shell_operation}=    Cumulocity.Operation Should Be SUCCESSFUL    ${shell_operation}    timeout=60

    ${result_text}=    Set Variable    ${shell_operation}[c8y_Command][result]
    Should Contain    ${result_text}    active

Get tedge smartrest config
    ${shell_operation}=    Execute Shell Command
    ...    tedge config get c8y.smartrest.templates
    ${shell_operation}=    Cumulocity.Operation Should Be SUCCESSFUL    ${shell_operation}
    RETURN    ${shell_operation["c8y_Command"]["result"].strip()}
