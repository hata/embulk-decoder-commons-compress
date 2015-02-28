Embulk::JavaPlugin.register_decoder(
  "commons-compress", "org.embulk.decoder.CommonsCompressDecoderPlugin",
  File.expand_path('../../../../classpath', __FILE__))
