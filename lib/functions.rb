#!/bin/env ruby
# encoding: UTF-8

def log(logfile,message,stamp)
        File.open(logfile, 'a+') do |f|
          if stamp
            f.puts "#{Time.now} -- #{message}"
          else
            f.puts message
          end
        end
end

def lxcode(language)
        case language
                when "Arabic"
                        iso = "AR"
                when "Chinese"
                        iso = "ZH"
                when "English"
                        iso = "EN"
                when "French"
                        iso = "FR"
                when "Russian"
                        iso = "RU"
                when "Spanish"
                        iso = "ES"
        end
        return iso
end

def s3_latest_csv(s3,bucket,prefix,pattern)
  #puts pattern
  files = Array.new
  epoch_time = DateTime.parse('1970-01-01T00:00:00+00:00')
  latest_time = epoch_time
  latest_file = nil
  s3.buckets[bucket].objects.with_prefix(prefix).each do |obj|
    if obj.key.downcase =~ /^#{prefix.downcase}\/#{pattern}/
      files << obj
    end
  end
  files.each do |file|
    mtime = file.last_modified
    if mtime > latest_time
      latest_file = file
    end
  end
  if latest_file
    return latest_file
  else
    return nil
  end
end

def s3_list_files_in_folder(s3,bucket,prefix,pattern)
  puts "\t#{s3}, #{bucket}, #{prefix}, #{pattern}"
  files = Array.new
  s3.buckets[bucket].objects.with_prefix(prefix).each do |obj|
    if obj.key.downcase =~ /^#{prefix.downcase}\/#{pattern}/
      #puts "Found file #{obj.key}"
      files << obj
    end
  end
  return files
end

def parse_csv(s3, s3_bucket, fname)
    items_array = Array.new
    metadata = SmarterCSV.process(fname, { :col_sep => "\t", :file_encoding => 'utf-8', :verbose => true })
    metadata.each do |row|
      item_hash = Hash.new
      item_hash["DocumentNumber"] = row[:doc_num]
      item_hash["JobNumber"] = row[:job_num]
      if row[:title]
        item_hash["Title"] = row[:title]
      end
      item_hash["Language"] = row[:lang]
      item_hash["DocumentSymbol"] = row[:symbol]
      item_hash["PublicationDate"] = row[:publication_date].gsub(/\-/,'').to_i
      item_hash["ReleaseDate"] = row[:release_date].gsub(/\-/,'').to_i
      if row[:agen_num]
        item_hash["AgendaNumber"] = row[:agen_num]
      end
      item_hash["Distribution"] = row[:distribution]
      if row[:isbn]
        item_hash["ISBN"] = row[:isbn]
      end
      if row[:issn]
        item_hash["ISSN"] = row[:issn]
      end
      if row[:cr_sales_num]
        item_hash["CRSalesNumber"] = row[:cr_sales_num]
      end
      fn = row[:job_num].gsub(/NY\-J\-/,'N').gsub(/\-/,'').gsub(/\*/,'')
      filename = "Drop/#{fn}.pdf"
      item_hash["Filename"] = filename
      if s3.buckets[s3_bucket].objects[filename].exists?
        file_status = "Found"
      else
        file_status = "Missing"
      end
      item_hash["FileStatus"] = file_status
      puts "For #{item_hash['DocumentSymbol']} (#{item_hash['Language']}), referenced file #{item_hash['Filename']} was #{file_status}."
      items_array << item_hash
      #item = db_table.items.create( item_hash )
    end
  return items_array
end
