require "google/spreadsheet/master/version"
require 'google_drive/alias'

module Google
  module Spreadsheet
    module Master
      APPLICATION_NAME     = 'master'
      TOKEN_CREDENTIAL_URI = 'https://accounts.google.com/o/oauth2/token'
      AUDIENCE             = 'https://accounts.google.com/o/oauth2/token'
      SCOPE                = 'https://www.googleapis.com/auth/drive https://spreadsheets.google.com/feeds https://docs.google.com/feeds'

      class Client
        def initialize(issuer, pem_path='client.pem')
          @issuer      = issuer
          @signing_key = Google::APIClient::KeyUtils.load_from_pem(pem_path, 'notasecret')
        end

        def client
          client = Google::APIClient.new(:application_name => APPLICATION_NAME)
          client.authorization = Signet::OAuth2::Client.new(
            :token_credential_uri => TOKEN_CREDENTIAL_URI,
            :audience             => AUDIENCE,
            :scope                => SCOPE,
            :issuer               => @issuer,
            :signing_key          => @signing_key,
          )
          client.authorization.fetch_access_token!
          return client
        end

        def access_token
          return self.client.authorization.access_token
        end

        def session
          return GoogleDrive.login_with_oauth(self.access_token)
        end

        def compare_header?(title, ss_key_1, ss_key_2)
          session = self.session
          ws1 = session.spreadsheet_by_key(ss_key_1).worksheet_by_title(title)
          ws2 = session.spreadsheet_by_key(ss_key_2).worksheet_by_title(title)
          return ws1.header == ws2.header
        end

        def merge(titles=[], base_ss_key, diff_ss_key)
          session = self.session

          base_ss = session.spreadsheet_by_key(base_ss_key)
          diff_ss = session.spreadsheet_by_key(diff_ss_key)

          titles.each do |title|
            base_ws = base_ss.worksheet_by_title(title)
            diff_ws = diff_ss.worksheet_by_title(title)

            diff_rows = diff_ws.populated_rows
            diff_rows.each do |diff_row|
              row = base_ws.append_row
              diff_ws.header.each do |column|
                row.send("#{column}=", diff_row.send("#{column}"))
              end
            end

            base_ws.save
          end
        end

        def backup(base_collection_url, backup_collection_name="backup")
          session = self.session

          base_collection   = session.collection_by_url(base_collection_url)
          backup_collection = base_collection.create_subcollection(backup_collection_name)

          base_collection.files.each do |base_file|
            if base_file.class == GoogleDrive::Spreadsheet then
              file = base_file.duplicate(base_file.title)
              backup_collection.add(file)
            end
          end
        end
      end
    end
  end
end
