module SalesforceBulkApi

  class Job
    attr_reader :job_id

    class SalesforceException < StandardError; end

    def initialize(args)
      @job_id         = args[:job_id]
      @operation      = args[:operation]
      @sobject        = args[:sobject]
      @external_key   = args[:external_key]
      @records        = args[:records]
      @connection     = args[:connection]
      @serial         = args.fetch(:serial, false)
      @batch_ids      = []
      @XML_HEADER     = '<?xml version="1.0" encoding="utf-8" ?>'
    end



    def create_job(batch_size, send_nulls, no_null_list)
      @batch_size = batch_size
      @send_nulls = send_nulls
      @no_null_list = no_null_list

      xml = "#{@XML_HEADER}<jobInfo xmlns=\"http://www.force.com/2009/06/asyncapi/dataload\">"
      xml += "<operation>#{@operation}</operation>"
      xml += "<object>#{@sobject}</object>"

      xml += "<externalIdFieldName>#{@external_key}</externalIdFieldName>" if @external_key && @operation == 'upsert'
      xml += "<concurrencyMode>Serial</concurrencyMode>" if @serial

      xml += "<contentType>XML</contentType>"
      xml += "</jobInfo>"

      path = "job"
      headers = Hash['Content-Type' => 'application/xml; charset=utf-8']

      response = @connection.post_xml(nil, path, xml, headers)
      response_parsed = XmlSimple.xml_in(response)

      # response may contain an exception, so raise it
      raise SalesforceException.new("#{response_parsed['exceptionMessage'][0]} (#{response_parsed['exceptionCode'][0]})") if response_parsed['exceptionCode']

      @job_id = response_parsed['id'][0]

    end

    def close_job()
      xml = "#{@XML_HEADER}<jobInfo xmlns=\"http://www.force.com/2009/06/asyncapi/dataload\">"
      xml += "<state>Closed</state>"
      xml += "</jobInfo>"

      path = "job/#{@job_id}"
      headers = Hash['Content-Type' => 'application/xml; charset=utf-8']

      response = @connection.post_xml(nil, path, xml, headers)
      XmlSimple.xml_in(response)
    end

    def add_query
      path = "job/#{@job_id}/batch/"
      headers = Hash["Content-Type" => "application/xml; charset=UTF-8"]

      response = @connection.post_xml(nil, path, @records, headers)
      response_parsed = XmlSimple.xml_in(response)

      @batch_ids << response_parsed['id'][0]
    end

    def add_batches
      raise 'Records must be an array of hashes.' unless @records.is_a? Array
      keys = @records.reduce({}) {|h, pairs| pairs.each {|k, v| (h[k] ||= []) << v}; h}.keys

      @records_dup = @records.clone

      super_records = []
      (@records_dup.size/@batch_size).to_i.times do
        super_records << @records_dup.pop(@batch_size)
      end
      super_records << @records_dup unless @records_dup.empty?

      super_records.each do |batch|
        @batch_ids << add_batch(keys, batch)
      end
    end

    def add_batch(keys, batch)
      xml = "#{@XML_HEADER}<sObjects xmlns=\"http://www.force.com/2009/06/asyncapi/dataload\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
      batch.each do |r|
        xml += create_sobject(keys, r)
      end
      xml += '</sObjects>'
      path = "job/#{@job_id}/batch/"
      headers = Hash["Content-Type" => "application/xml; charset=UTF-8"]
      response = @connection.post_xml(nil, path, xml, headers)
      response_parsed = XmlSimple.xml_in(response)
      response_parsed['id'][0] if response_parsed['id']
    end

    def build_sobject(data)
      xml = '<sObject>'
      data.each do |key, value|
        if key.is_a?(Hash)
          xml += build_sobject(key)
        elsif value != :type
          xml += "<#{key}>#{encode_xml_value(value)}</#{key}>"
        end
      end
      xml += '</sObject>'
      xml
    end

    def create_sobject(keys, record)
      sobject_xml = '<sObject>'
      keys.each do |key|
        value = record[key]

        if value.is_a?(Hash)
          sobject_xml += "<#{key}>"
          sobject_xml += build_sobject(value)
          sobject_xml += "</#{key}>"
        elsif !value.to_s.empty?
          sobject_xml += "<#{key}>"
          sobject_xml += encode_xml_value(value)
          sobject_xml += "</#{key}>"
        elsif @send_nulls && !@no_null_list.include?(key)
          sobject_xml += "<#{key} xsi:nil=\"true\"/>"
        end
      end
      sobject_xml += '</sObject>'
      sobject_xml
    end

    def encode_xml_value(value)
      if value.respond_to?(:encode)
        value.encode(xml: :text)
      elsif value.respond_to?(:iso8601) # timestamps
        value.iso8601.to_s
      else
        value.to_s.encode(xml: :text)
      end
    end

    def check_job_status
      path = "job/#{@job_id}"
      headers = Hash.new
      response = @connection.get_request(nil, path, headers)

      begin
        response_parsed = XmlSimple.xml_in(response) if response
        response_parsed
      rescue StandardError => e
        puts "Error parsing XML response for #{@job_id}"
        puts e
        puts e.backtrace
      end
    end

    def check_batch_status(batch_id)
      path = "job/#{@job_id}/batch/#{batch_id}"
      headers = Hash.new

      response = @connection.get_request(nil, path, headers)

      begin
        response_parsed = XmlSimple.xml_in(response) if response
        response_parsed
      rescue StandardError => e
        puts "Error parsing XML response for #{@job_id}, batch #{batch_id}"
        puts e
        puts e.backtrace
      end
    end

    def get_job_result(return_result, timeout)
      # timeout is in seconds
      begin
        state = []
        Timeout::timeout(timeout, SalesforceBulkApi::JobTimeout) do
          while true
            if self.check_job_status['state'][0] == 'Closed'
              @batch_ids.each do |batch_id|
                batch_state = self.check_batch_status(batch_id)
                if batch_state['state'][0] != "Queued" && batch_state['state'][0] != "InProgress"
                  state << (batch_state)
                  @batch_ids.delete(batch_id)
                end
              end
              break if @batch_ids.empty?
            else
              break
            end
          end
        end
      rescue SalesforceBulkApi::JobTimeout => e
        puts 'Timeout waiting for Salesforce to process job batches #{@batch_ids} of job #{@job_id}.'
        puts e
        raise
      end

      state.each_with_index do |batch_state, i|
        if batch_state['state'][0] == 'Completed' && return_result == true
          state[i].merge!({'response' => self.get_batch_result(batch_state['id'][0])})
        end
      end
      state
    end

    def get_batch_result(batch_id)
      path = "job/#{@job_id}/batch/#{batch_id}/result"
      headers = Hash["Content-Type" => "application/xml; charset=UTF-8"]

      response = @connection.get_request(nil, path, headers)
      response_parsed = XmlSimple.xml_in(response)
      results = response_parsed['result'] unless @operation == 'query'

      if(@operation == 'query') # The query op requires us to do another request to get the results
        result_id = response_parsed["result"][0]
        path = "job/#{@job_id}/batch/#{batch_id}/result/#{result_id}"
        headers = Hash.new
        headers = Hash["Content-Type" => "application/xml; charset=UTF-8"]
        response = @connection.get_request(nil, path, headers)
        response_parsed = XmlSimple.xml_in(response)
        results = response_parsed['records']
      end
      results
    end

  end

  class JobTimeout < StandardError
  end
end
