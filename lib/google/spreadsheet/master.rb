require "google/spreadsheet/master/version"
require 'google_drive/alias'

module GoogleDrive
  class Spreadsheet
    define_method 'can_merge?' do |target_ss, ws_titles=[]|
      ws_titles.each do |ws_title|
        base_ws   = self.worksheet_by_title(ws_title)
        target_ws = target_ss.worksheet_by_title(ws_title)
        unless base_ws.same_header?(target_ws) then
          return false
        end
      end
      return true
    end

    define_method 'merge' do |diff_ss, ws_titles=[]|
      unless self.can_merge?(diff_ss, ws_titles) then
        raise "can not merge ss: #{ss.title}"
      end

      ws_titles.each do |ws_title|
        base_ws = self.worksheet_by_title(ws_title)
        diff_ws = diff_ss.worksheet_by_title(ws_title)

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
  end

  class Worksheet
    define_method 'same_header?' do |ws|
      return self.header == ws.header
    end
  end
end

module Google
  module Spreadsheet
    module Master
      APPLICATION_NAME     = 'master'
      TOKEN_CREDENTIAL_URI = 'https://accounts.google.com/o/oauth2/token'
      AUDIENCE             = 'https://accounts.google.com/o/oauth2/token'
      SCOPE                = 'https://www.googleapis.com/auth/drive https://spreadsheets.google.com/feeds https://docs.google.com/feeds'

      INDEX_WS_TITLE_DEFAULT = 'table_map'
      attr_accessor :index_ws_title

      class Client
        def initialize(issuer, pem_path='client.pem')
          @issuer         = issuer
          @signing_key    = Google::APIClient::KeyUtils.load_from_pem(pem_path, 'notasecret')
          @index_ws_title = INDEX_WS_TITLE_DEFAULT
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

        def merge(base_ss_key, diff_ss_key, ws_titles=[])
          session = self.session

          base_ss = session.spreadsheet_by_key(base_ss_key)
          diff_ss = session.spreadsheet_by_key(diff_ss_key)

          base_ss.merge(diff_ss, ws_titles)
        end

        def backup(base_index_ss_key, base_collection_url, backup_collection_name="backup")
          session = self.session

          base_collection   = session.collection_by_url(base_collection_url)
          backup_collection = base_collection.create_subcollection(backup_collection_name)

          base_index_ss = session.spreadsheet_by_key(base_index_ss_key)
          base_index_ws = base_index_ss.worksheet_by_title(@index_ws_title)

          backup_index_ss = base_index_ss.duplicate(base_index_ss.title)
          backup_index_ws = backup_index_ss.worksheet_by_title(@index_ws_title)

          backup_collection.add(backup_index_ss)

          base_ss_keys = base_index_ws.populated_rows.map { |row| row.key }
          base_ss_keys.uniq.each do |base_ss_key|
            base_ss   = session.spreadsheet_by_key(base_ss_key)
            backup_ss = base_ss.duplicate(base_ss.title)

            backup_index_ws.populated_rows.each do |row|
              if row.key == base_ss_key then
                row.key = backup_ss.key
              end
            end

            backup_collection.add(backup_ss)
          end

          backup_index_ws.save

          return backup_collection
        end
      end
    end
  end
end
