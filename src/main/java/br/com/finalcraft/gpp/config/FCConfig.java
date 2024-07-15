package br.com.finalcraft.gpp.config;

import br.com.finalcraft.evernifecore.config.Config;

import java.io.File;

public class FCConfig extends Config {

	public FCConfig(File theFile) {
		super(theFile);
		this.enableSmartCache();
	}

}
