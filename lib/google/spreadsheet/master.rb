require 'logger'
require 'google/spreadsheet/master/version'
require 'google_drive/alias'

module Google
  module Spreadsheet
    module Master
      class Client
        APPLICATION_NAME          = 'google-spreadsheet-master'
        TOKEN_CREDENTIAL_URI      = 'https://accounts.google.com/o/oauth2/token'
        AUDIENCE                  = 'https://accounts.google.com/o/oauth2/token'
        SCOPE                     = 'https://www.googleapis.com/auth/drive https://spreadsheets.google.com/feeds https://docs.google.com/feeds'
        ORIGINAL_COLLECTION_TITLE = 'release'
        BACKUP_COLLECTION_TITLE   = 'backup'
        INDEX_WS_TITLE_DEFAULT    = 'table_map'
        ROW_OFFSET_DEFAULT        = 0
        UPDATE_ROW_NUM_DEFAULT    = 5

        attr_accessor :index_ws_title, :logger, :row_offset

        def initialize(issuer, pem_path='client.pem')
          @issuer         = issuer
          @signing_key    = Google::APIClient::KeyUtils.load_from_pem(pem_path, 'notasecret')
          @index_ws_title = INDEX_WS_TITLE_DEFAULT
          @logger         = Logger.new(STDOUT)
          @row_offset     = ROW_OFFSET_DEFAULT
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
          unless self.instance_variable_defined?(:@session) then
            @session = GoogleDrive.login_with_oauth(self.access_token)
          end

          return @session
        end

        def merge(base_ss_key, diff_ss_key, ws_title)
          @logger.info "#{ws_title} : start merge"

          session = self.session

          base_ss = session.spreadsheet_by_key(base_ss_key)
          diff_ss = session.spreadsheet_by_key(diff_ss_key)

          base_ss.merge(diff_ss, ws_title, @row_offset)

          @logger.info "#{ws_title} : finish merge"
        end

        def merge_by_index_ws(base_index_ws, diff_index_ws, sheetname)
          session = self.session

          base_index_rows = base_index_ws.populated_rows
          diff_index_rows = diff_index_ws.populated_rows

          target_diff_index_rows = diff_index_rows.select { |row| row.sheetname == sheetname }
          raise "#{sheetname} : sheetname duplication in diff index ws" if target_diff_index_rows.count > 1

          diff_index_row = target_diff_index_rows.first
          raise "#{sheetname} : no corresponding diff index row" unless diff_index_row

          target_base_index_rows = base_index_rows.select { |row| row.sheetname == sheetname }
          raise "#{sheetname} : sheetname duplication in base index ws" if target_base_index_rows.count > 1

          base_index_row = target_base_index_rows.first
          raise "#{sheetname} : no corresponding base index row" unless base_index_row
          raise "#{sheetname} : same key" if base_index_row.key == diff_index_row.key

          @logger.info "#{sheetname} : start check"

          base_ws = session.spreadsheet_by_key(base_index_row.key).worksheet_by_title(sheetname)
          diff_ws = session.spreadsheet_by_key(diff_index_row.key).worksheet_by_title(sheetname)

          raise "#{diff_ws.title} : different header" unless diff_ws.same_header?(base_ws)

          base_ids = base_ws.populated_rows.select { |row| !row.id.empty? }.map { |row| row.id }
          diff_ids = diff_ws.populated_rows.select { |row| !row.id.empty? }.map { |row| row.id }

          all_ids  = base_ids + diff_ids
          uniq_ids = all_ids.uniq

          raise "#{sheetname} : id duplication" if all_ids.size != uniq_ids.size

          @logger.info "#{sheetname} : finish check"

          self.merge(base_index_row.key, diff_index_row.key, sheetname)
        end

        def merge_by_index_ss_key(base_index_ss_key, diff_index_ss_key, sheetname)
          session = self.session

          base_index_ss = session.spreadsheet_by_key(base_index_ss_key)
          base_index_ws = base_index_ss.worksheet_by_title(@index_ws_title)

          diff_index_ss = session.spreadsheet_by_key(diff_index_ss_key)
          diff_index_ws = diff_index_ss.worksheet_by_title(@index_ws_title)

          begin
            self.merge_by_index_ws(base_index_ws, diff_index_ws, sheetname)
          rescue => e
            @logger.fatal e.message
          end
        end

        def dry_merge_by_index_ss_key(base_index_ss_key, diff_index_ss_key, base_collection_url)
          session = self.session

          begin
            backup_collection, backup_index_ws = self.backup(base_index_ss_key, base_collection_url)
          rescue => e
            @logger.fatal e.message
          end

          diff_index_ss = session.spreadsheet_by_key(diff_index_ss_key)
          diff_index_ws = diff_index_ss.worksheet_by_title(@index_ws_title)

          begin
            self.merge_by_index_ws(backup_index_ws, diff_index_ws)
          rescue => e
            backup_collection.delete
            @logger.fatal e.message
          end
        end

        def backup(index_ss_key, base_collection_url, collection_title)
          @logger.info 'start backup'

          session = self.session

          base_collection   = session.collection_by_url(base_collection_url)
          origin_collection = base_collection.subcollection_by_title(ORIGINAL_COLLECTION_TITLE)
          backup_collection = base_collection.subcollection_by_title(BACKUP_COLLECTION_TITLE)
          collection        = backup_collection.create_subcollection(collection_title)

          index_ss = session.spreadsheet_by_key(index_ss_key)
          index_ws = index_ss.worksheet_by_title(@index_ws_title)

          backup_index_ss = index_ss.copy(index_ss.title)
          backup_index_ws = backup_index_ss.worksheet_by_title(@index_ws_title)

          collection.add(backup_index_ss)
          @logger.info "#{index_ss.title} : done"

          ss_keys = index_ws.populated_rows.map { |row| row.key }.uniq
          ss_keys.each do |ss_key|
            ss        = session.spreadsheet_by_key(ss_key)
            backup_ss = ss.copy(ss.title)

            backup_index_ws.populated_rows.each do |row|
              if row.key == ss_key then
                row.key = backup_ss.key
              end
            end

            collection.add(backup_ss)
            origin_collection.remove(backup_ss)
            @logger.info "#{ss.title} : done"
          end

          backup_ss_keys = backup_index_ws.populated_rows.map { |row| row.key }.uniq
          ss_keys.each do |ss_key|
            if backup_ss_keys.include?(ss_key) then
              collection.delete
              raise 'fail in duplication'
            end
          end

          backup_index_ws.save
          origin_collection.remove(backup_index_ss)

          @logger.info 'finish backup'

          return backup_collection, backup_index_ws
        end
      end
    end
  end
end

module GoogleDrive
  class Spreadsheet
    define_method 'merge' do |diff_ss, ws_title, offset=0|
      base_ws = self.worksheet_by_title(ws_title)
      diff_ws = diff_ss.worksheet_by_title(ws_title)

      diff_rows = diff_ws.populated_rows

      count = 0
      diff_rows.each do |diff_row|
        next if diff_row.id.empty?

        count += 1

        if count == 1 && offset > 0 then
          row = base_ws.append_row(offset)
        else
          row = base_ws.append_row
        end

        diff_ws.header.select { |column| !column.empty? }.each do |column|
          row.send("#{column}=", "'#{diff_row.send(column)}")
        end

        row_update_num = 5
        if count % row_update_num == 0 then
          base_ws.save
        end
      end

      base_ws.save
    end
  end

  class Worksheet
    define_method 'same_header?' do |target_ws|
      self_header   = self.header.select { |column| !column.empty? }
      target_header = target_ws.header.select { |column| !column.empty? }

      return self_header == target_header
    end
  end
end
