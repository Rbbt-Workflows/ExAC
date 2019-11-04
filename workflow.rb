require 'rbbt'
require 'rbbt/workflow'

module ExAC
  extend Workflow

  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/jan2013"

  input :mutations, :array, "Genomic Mutation", nil, :stream => true
  input :by_position, :boolean, "Identify by position", false
  task :annotate => :tsv do |mutations,by_position|
    database = ExAC.database
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => database.fields, :type => (by_position ? :double : :list), :organism => ExAC.organism
    dumper.init
    database.unnamed = true
    TSV.traverse mutations, :type => :array, :into => dumper, :bar => self.progress_bar("Annotate ExAC") do |mutation|
      if by_position
        position = mutation.split(":").values_at(0,1) * ":"
        keys = database.prefix(position + ":")
        next if keys.nil?
        values = keys.collect{|key| database[key].collect{|v| v.nil? ? nil : v.gsub("|", ";") } }.uniq
        [mutation, Misc.zip_fields(values)]
      else
        values = database[mutation]
        next if values.nil?
        values = values.collect{|v| v.nil? ? nil : v.gsub("|", ";") } 
        [mutation, values]
      end
    end
  end
end

require 'rbbt/sources/ExAC'
