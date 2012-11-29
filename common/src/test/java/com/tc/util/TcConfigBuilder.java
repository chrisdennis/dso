package com.tc.util;

import org.apache.commons.io.IOUtils;
import org.apache.xmlbeans.XmlOptions;

import com.tc.config.Loader;
import com.terracottatech.config.Property;
import com.terracottatech.config.Server;
import com.terracottatech.config.TcConfigDocument;
import com.terracottatech.config.TcConfigDocument.TcConfig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class TcConfigBuilder {

  private final TcConfigDocument tcConfigDocument;
  private final TcConfig         tcConfig;
  private XmlOptions             xmlOptions;
  private File                   tcConfigFile = new File("tc-config.xml");

  public File getTcConfigFile() {
    return tcConfigFile;
  }

  public void setTcConfigFile(File tcConfigFile) {
    this.tcConfigFile = tcConfigFile;
  }

  public TcConfigBuilder() {
    this("tcconfigbuilder-default.xml");
  }

  public TcConfigBuilder(String resourcePath) {
    InputStream in = TcConfigBuilder.class.getResourceAsStream(resourcePath);
    if (in == null) { throw new IllegalArgumentException("no resource available for " + resourcePath); }

    try {
      tcConfigDocument = new Loader().parse(in);
      tcConfig = tcConfigDocument.getTcConfig();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public TcConfigBuilder(File file) {
    try {
      tcConfigDocument = new Loader().parse(file);
      tcConfig = tcConfigDocument.getTcConfig();
      tcConfigFile = file;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private TcConfigBuilder(TcConfigDocument tcd) {
    tcConfigDocument = tcd;
    tcConfig = tcConfigDocument.getTcConfig();
  }

  public Server[] getServers() {
    return tcConfig.getServers().getServerArray();
  }

  public InputStream newInputStream() {
    return tcConfigDocument.newInputStream(getXmlOptions());
  }

  public void setDsoHost(String host) {
    ensureServers();
    tcConfig.getServers().getServerArray(0).setHost(host);
  }

  public String getDsoHost() {
    ensureServers();
    return tcConfig.getServers().getServerArray(0).getHost();
  }

  public void setTsaPort(int portNo) {
    setTsaPort(0, portNo);
  }

  public void setTsaPort(int serverIndex, int portNo) {
    ensureServers();
    Server server = tcConfig.getServers().getServerArray(serverIndex);
    Assert.assertNotNull(server);
    if (!server.isSetTsaPort()) {
      server.addNewTsaPort();
    }
    server.getTsaPort().setIntValue(portNo);
  }

  public void setGroupPort(int portNo) {
    setGroupPort(0, portNo);
  }

  public void setGroupPort(int serverIndex, int portNo) {
    ensureServers();
    Server server = tcConfig.getServers().getServerArray(serverIndex);
    Assert.assertNotNull(server);
    if (!server.isSetTsaGroupPort()) {
      server.addNewTsaGroupPort();
    }
    server.getTsaGroupPort().setIntValue(portNo);
  }

  public int getTsaPort() {
    return getTsaPort(0);
  }

  public int getTsaPort(int serverIndex) {
    ensureServers();
    Assert.assertNotNull(tcConfig.getServers().getServerArray(serverIndex));
    return tcConfig.getServers().getServerArray(serverIndex).getTsaPort().getIntValue();
  }

  public int getGroupPort() {
    return getGroupPort(0);
  }

  public int getGroupPort(int serverIndex) {
    ensureServers();
    Assert.assertNotNull(tcConfig.getServers().getServerArray(serverIndex));
    return tcConfig.getServers().getServerArray(serverIndex).getTsaGroupPort().getIntValue();
  }

  public void setJmxPort(int portNo) {
    setJmxPort(0, portNo);
  }

  public void setJmxPort(int serverIndex, int portNo) {
    ensureServers();
    Server server = tcConfig.getServers().getServerArray(serverIndex);
    Assert.assertNotNull(server);
    if (!server.isSetJmxPort()) {
      server.addNewJmxPort();
    }
    server.getJmxPort().setIntValue(portNo);
  }

  public int getJmxPort() {
    return getJmxPort(0);
  }

  public int getJmxPort(int serverIndex) {
    ensureServers();
    Assert.assertNotNull(tcConfig.getServers().getServerArray(serverIndex));
    return tcConfig.getServers().getServerArray(serverIndex).getJmxPort().getIntValue();
  }

  public void addProperty(String key, String value) {
    ensureTcProprties();
    Property tcProps = tcConfig.getTcProperties().addNewProperty();
    tcProps.setName(key);
    tcProps.setValue(value);
  }

  public void setServerLogs(String path) {
    ensureServers();
    tcConfig.getServers().getServerArray(0).setLogs(path);
  }

  public void setServerData(String path) {
    ensureServers();
    tcConfig.getServers().getServerArray(0).setData(path);
  }

  public void setServerLogs(int index, String path) {
    ensureServers();
    tcConfig.getServers().getServerArray(index).setLogs(path);
  }

  public void setServerData(int index, String path) {
    ensureServers();
    tcConfig.getServers().getServerArray(index).setData(path);
  }

  public void setClientLogs(String path) {
    ensureClients();
    tcConfig.getClients().setLogs(path);
  }

  public void randomizePorts() {
    PortChooser pc = new PortChooser();
    Server[] servers = getServers();
    for (Server server : servers) {
      if (!server.isSetTsaPort()) server.addNewTsaPort();
      server.getTsaPort().setIntValue(pc.chooseRandomPort());

      if (!server.isSetJmxPort()) server.addNewJmxPort();
      server.getJmxPort().setIntValue(pc.chooseRandomPort());

      if (!server.isSetTsaGroupPort()) server.addNewTsaGroupPort();
      server.getTsaGroupPort().setIntValue(pc.chooseRandomPort());
    }
  }

  @Override
  public String toString() {
    return tcConfigDocument.toString();
  }

  public void saveToFile() throws IOException {
    InputStream is = null;
    FileOutputStream fos = null;
    try {
      is = tcConfigDocument.newInputStream(getXmlOptions());
      fos = new FileOutputStream(tcConfigFile);
      IOUtils.copy(tcConfigDocument.newInputStream(getXmlOptions()), fos);
    } finally {
      IOUtils.closeQuietly(fos);
      IOUtils.closeQuietly(is);
    }
  }

  private XmlOptions getXmlOptions() {
    if (xmlOptions == null) {
      xmlOptions = new XmlOptions();
      xmlOptions.setLoadLineNumbers();
      xmlOptions.setSavePrettyPrint();
      xmlOptions.setSavePrettyPrintIndent(2);
    }
    return xmlOptions;
  }

  private void ensureServers() {
    if (!tcConfig.isSetServers()) {
      tcConfig.addNewServers();
    }
  }

  private void ensureTcProprties() {
    if (!tcConfig.isSetTcProperties()) {
      tcConfig.addNewTcProperties();
    }
  }

  private void ensureClients() {
    if (!tcConfig.isSetClients()) {
      tcConfig.addNewClients();
    }
  }

  public TcConfigBuilder copy() {
    try {
      TcConfigBuilder aCopy = new TcConfigBuilder(new Loader().parse(this.toString()));
      return aCopy;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
