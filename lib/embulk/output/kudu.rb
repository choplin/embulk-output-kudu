Embulk::JavaPlugin.register_output(
  "kudu", "org.embulk.output.kudu.KuduOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
