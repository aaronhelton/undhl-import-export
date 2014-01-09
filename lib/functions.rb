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
lang = Hash.new
case language
  when "A"
    lang["short"] = "A"
    lang["iso"] = "AR"
    lang["long"] = "Arabic"
  when "AR"
    lang["short"] = "A"
    lang["iso"] = "AR"
    lang["long"] = "Arabic"
  when "Arabic" 
    lang["short"] = "A"
    lang["iso"] = "AR"
    lang["long"] = "Arabic"
  when "C"
    lang["short"] = "C"
    lang["iso"] = "ZH"
    lang["long"] = "Chinese"
  when "ZH"
    lang["short"] = "C"
    lang["iso"] = "ZH"
    lang["long"] = "Chinese"
  when "Chinese"
    lang["short"] = "C"
    lang["iso"] = "ZH"
    lang["long"] = "Chinese"
  when "E"
    lang["short"] = "E"
    lang["iso"] = "EN"
    lang["long"] = "English"
  when "EN"
    lang["short"] = "E"
    lang["iso"] = "EN"
    lang["long"] = "English"
  when "English"
    lang["short"] = "E"
    lang["iso"] = "EN"
    lang["long"] = "English"
  when "F"
    lang["short"] = "F"
    lang["iso"] = "FR"
    lang["long"] = "French"
  when "FR"
    lang["short"] = "F"
    lang["iso"] = "FR"
    lang["long"] = "French"
  when "French"
    lang["short"] = "F"
    lang["iso"] = "FR"
    lang["long"] = "French"
  when "R"
    lang["short"] = "R"
    lang["iso"] = "RU"
    lang["long"] = "Russian"
  when "RU"
    lang["short"] = "R"
    lang["iso"] = "RU"
    lang["long"] = "Russian"
  when "Russian"
    lang["short"] = "R"
    lang["iso"] = "RU"
    lang["long"] = "russian"
  when "S"
    lang["short"] = "S"
    lang["iso"] = "ES"
    lang["long"] = "Spanish"
  when "ES"
    lang["short"] = "S"
    lang["iso"] = "ES"
    lang["long"] = "Spanish"
  when "Spanish"
    lang["short"] = "S"
    lang["iso"] = "ES"
    lang["long"] = "Spanish"
  else
    lang["short"] = "O"
    lang["iso"] = "ZZ"
    lang["long"] = "Other"
end
return lang
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
  #puts "\t#{s3}, #{bucket}, #{prefix}, #{pattern}"
  files = Array.new
  s3.buckets[bucket].objects.with_prefix(prefix).each do |obj|
    if obj.key.downcase =~ /^#{prefix.downcase}\/#{pattern}/
      #puts "Found file #{obj.key}"
      files << obj
    end
  end
  return files
end

def s3_move_file(s3, s3_bucket, old_key, new_key)
  old = s3.buckets[s3_bucket].objects[old_key]
  new = old.move_to(new_key)
end

#def s3_write_file(s3,bucket,key,data)
#  s3.buckets[bucket].objects.create(key,data)
#end

def get_agenda(agenda_key)
  agenda_text = nil
  a = nil
  xmldoc = REXML::Document.new File.read 'lib/agenda.xml'
  unless agenda_key == "*"
    a = xmldoc.elements["//agendas/agenda/item[@id='#{agenda_key}']/label"].text
  end
  if a
    return a
  else
    return nil
  end
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

def make_dublin_core_xml(metadata,languages)
  xml = nil
  title = nil
  docsymbol = nil
  agenda = nil
  isbn = nil
  issn = nil
  #First determine what we even have
  if metadata["Title"] && metadata["Title"][:s]
    title = metadata["Title"][:s]
  elsif metadata["DocumentSymbol"] && metadata["DocumentSymbol"][:s]
    title = metadata["DocumentSymbol"][:s]
  else
    title = "Untitled"
  end
  if  metadata["ISSN"] && metadata["ISSN"][:s]
    issn =  metadata["ISSN"][:s]
  end
  if metadata["ISBN"] && metadata["ISBN"][:s]
    isbn = metadata["ISBN"][:s]
  end
  issued_date = Date.parse(metadata["PublicationDate"][:n])
  xml = XML_HEADER
  xml = xml + "<dublin_core>" + "\n"
  xml = xml + "  <dcvalue element=\"title\" qualifier=\"none\">#{title}</dcvalue>" + "\n"
  xml = xml + "  <dcvalue element=\"date\" qualifier=\"issued\">#{issued_date}</dcvalue>" + "\n"
  languages.each do |language|
    xml = xml + "  <dcvalue element=\"language\" qualifier=\"none\">#{lxcode(language.to_s)['long']}</dcvalue>" + "\n"
  end
  xml = xml + "  <dcvalue element=\"type\" qualifier=\"none\">Parliamentary Document</dcvalue>" + "\n"
  if issn
    xml = xml + "  <dcvalue element=\"issn\" qualifier=\"none\">#{issn}</dcvalue>" + "\n"
  end
  if isbn
    xml = xml + "  <dcvalue element=\"isbn\" qualifier=\"none\">#{isbn}</dcvalue>" + "\n"
  end
  xml = xml + "</dublin_core>"
  return xml
end

def make_undr_xml(metadata)
  xml = nil
  docsymbol = nil
  agenda = nil
  agenda_doc = nil
  agenda_key = nil
  if metadata["DocumentSymbol"] && metadata["DocumentSymbol"][:s]
    docsymbol = metadata["DocumentSymbol"][:s]
    if docsymbol =~ /\/67\//
      agenda_doc = 'A67251'
    elsif docsymbol =~ /\/68\//
      agenda_doc = 'A68251'
    elsif docsymbol =~ /E\/.+\/2013\//
      agenda_doc = 'E2013100'
    end
  end
  if metadata["AgendaNumber"] && metadata["AgendaNumber"][:s] &&  metadata["AgendaNumber"][:s].size > 0
    if agenda_doc
      agenda_key = agenda_doc + metadata["AgendaNumber"][:s].gsub(/\(/, '').gsub(/\)/, '').gsub(/\s+/, '').gsub(/\*/,'')
    end
    if agenda_key
      agenda = get_agenda(agenda_key)
    end
  end
  xml = XML_HEADER
  xml = xml + "<dublin_core schema=\"undr\">" + "\n"
  if docsymbol
    xml = xml + "  <dcvalue element=\"docsymbol\" qualifier=\"none\">#{docsymbol}</dcvalue>" + "\n"
  end
  if agenda
    xml = xml + "  <dcvalue element=\"agenda\" qualifier=\"none\">#{agenda}</dcvalue>" + "\n"
  end
  xml = xml + "</dublin_core>"
  return xml
end
