# harpin AI Utilities
A collection of utilities to access and use the harpin toolkit.

# Setup - Authentication
Most harpin utilities require access credentials for API authentication.  These are created in the harpin web application and retrieved from local environment variables.

## Obtaining the client ID and refresh token
Follow these steps in the harpin AI web application to obtain the client ID and refresh token needed to get an access token.
1. Access the harpin AI web application at https://app.harpin.ai
1. Log in with your username and password. If you do not have credentials, reach out to your harpin AI account manager.
1. Once you are logged in, expand the "Settings" menu in the bottom left corner, and then click on the "Account settings" menu item.
1. On the "Account settings" screen, click on the "API credentials" tab.
1. Copy or note the “Client ID” value on this tab.
1. Use the "Generate refresh token" button to obtain a refresh token. Copy the refresh token somewhere secure, as once you leave the screen it cannot be obtained again. It will need to be regenerated.

## Setting local environment variables
These utilities retrieve the client ID and refresh token from environment variables named `HARPIN_CLIENT_ID` and `HARPIN_REFRESH_TOKEN`.  This approach allows them to be injected at runtime using a secrets manager utility.  **The Client ID and Refresh Token values grant privileged access to harpin APIs - make sure they are well-protected.**

### Windows
Open Command Prompt or PowerShell and run one of the following commands:
* Command Prompt
  * `set HARPIN_CLIENT_ID=your_client_id`
  * `set HARPIN_REFRESH_TOKEN=your_refresh_token`
* PowerShell
  * `$env:HARPIN_CLIENT_ID="your_client_id"`
  * `$env:HARPIN_REFRESH_TOKEN="your_refresh_token"`

Note that this will only persist for your current session. To make it permanent, set these values through System Properties > Environment Variables.

### macOS and Linux
Open your terminal and run:
* `export HARPIN_CLIENT_ID=your_client_id`
* `export HARPIN_REFRESH_TOKEN=your_refresh_token`

To make this permanent, add these lines to your shell configuration file (`~/.bashrc`, `~/.zshrc`, or `~/.bash_profile` depending on your shell). Then restart your terminal or run `source ~/.bashrc` (or the appropriate file) to apply the changes.
