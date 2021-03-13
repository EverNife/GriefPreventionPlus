package br.com.finalcraft.gpp.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.kaikk.mc.gpp.GriefPreventionPlus;
import org.bukkit.*;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class FCConfig {
	
	File theFile;
	FileConfiguration config;
	private static Random random = new Random();

	/**
	 * Creates a handlers Config Object for the specified File
	 *
	 * @param  theFile The File for which the Config object is created for
	 */
	public FCConfig(File theFile) {
		this.theFile = theFile;
		this.config = YamlConfiguration.loadConfiguration(this.theFile);
	}

	/**
	 * Returns the File the Config is handling
	 *
	 * @return      The File this Config is handling
	 */ 
	public File getTheFile() {
		return this.theFile;
	}
	
	/**
	 * Converts this Config Object into a plain FileConfiguration Object
	 *
	 * @return      The converted FileConfiguration Object
	 */ 
	public FileConfiguration getConfiguration() {
		return this.config;
	}
	
	protected void store(String path, Object value) {
		this.config.set(path, value);
	}
	
	/**
	 * Sets the Value for the specified Path
	 *
	 * @param  path The path in the Config File
	 * @param  value The Value for that Path
	 */
	public void setValue(String path, Object value) {
		if (path == null) throw new RuntimeException("Path is null my friend! Why?");
		if (value == null) {
			this.store(path, value);
			this.store(path + "_extra", null);
		}
		else if (value instanceof Inventory) {
			for (int i = 0; i < ((Inventory) value).getSize(); i++) {
				setValue(path + "." + i, ((Inventory) value).getItem(i));
			}
		}
		else if (value instanceof Date) {
			this.store(path, String.valueOf(((Date) value).getTime()));
		}
		else if (value instanceof Long) {
			this.store(path, String.valueOf(value));
		}
		else if (value instanceof UUID) {
			this.store(path, value.toString());
		}
		else if (value instanceof Sound) {
			this.store(path, String.valueOf(value));
		}
		else if (value instanceof ItemStack) {
			this.store(path, new ItemStack((ItemStack) value));
		}
		else if (value instanceof World) {
			this.store(path, ((World) value).getName());
		}
		else this.store(path, value);
	}
	
	/**
	 * Saves the Config Object to its File, and ensure its assync state
	 */
	public static ExecutorService scheduler = new ThreadPoolExecutor(5, 100,
			1000L, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue(),
			new ThreadFactoryBuilder().setNameFormat("assyncsave-pool-%d").build());

	public void saveAsync() {
		scheduler.submit(this::save);
	}

	private final ReentrantLock lock = new ReentrantLock(true);
	/**
	 * Saves the Config Object to its File
	 */
	public void save() {
		lock.lock();
		try {
			config.save(theFile);
		} catch (IOException e) {
			GriefPreventionPlus.addLogEntry("Failed to save file [" + theFile.getAbsolutePath() + "]");
			e.printStackTrace();
		}finally {
			lock.unlock();
		}
	}

	/**
	 * Checks whether the Config contains the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      True/false
	 */ 
	public boolean contains(String path) {
		return config.contains(path);
	}
	
	/**
	 * Returns the Object at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The Value at that Path
	 */ 
	public Object getValue(String path) {
		return config.get(path);
	}

	/**
	 * Returns the String at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The String at that Path
	 */
	public String getString(String path) {
		return config.getString(path);
	}
	public String getString(String path, String def) {
		return config.getString(path,def);
	}

	/**
	 * Returns the Integer at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The Integer at that Path
	 */
	public int getInt(String path) {
		return config.getInt(path);
	}
	public int getInt(String path, int def) {
		return config.getInt(path,def);
	}

	/**
	 * Returns the Boolean at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The Boolean at that Path
	 */
	public boolean getBoolean(String path) {
		return config.getBoolean(path);
	}
	public boolean getBoolean(String path,boolean def) {
		return config.getBoolean(path,def);
	}

	/**
	 * Returns the StringList at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The StringList at that Path
	 */
	public List<String> getStringList(String path) {
		return config.getStringList(path);
	}
    public List<String> getStringList(String path, List<String> def) {
        if (contains(path)) return config.getStringList(path);
        return def;
    }

	/**
	 * Returns the IntegerList at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The IntegerList at that Path
	 */ 
	public List<Integer> getIntList(String path) {
		return config.getIntegerList(path);
	}
	
	/**
	 * Recreates the File of this Config
	 */ 
	public void createFile() {
		try {
			this.theFile.createNewFile();
		} catch (IOException e) {
		}
	}
	
	/**
	 * Returns the Float at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The Float at that Path
	 */ 
	public Float getFloat(String path) {
		return Float.valueOf(String.valueOf(getValue(path)));
	}
	
	/**
	 * Returns the Long at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The Long at that Path
	 */ 
	public Long getLong(String path) {
		return Long.valueOf(String.valueOf(getValue(path)));
	}
	public Long getLong(String path, long def) {
		if (!contains(path)){
			return def;
		}
		return getLong(path);
	}

	/**
	 * Returns the UUID at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The UUID at that Path
	 */ 
	public UUID getUUID(String path) {
		if (contains(path)){
			return UUID.fromString(getString(path));
		}
		return null;
	}

	/**
	 * Returns the UUID at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The UUID at that Path
	 */
	public UUID getUUID(String path, UUID def) {
		return UUID.fromString(getString(path,def.toString()));
	}
	
	/**
	 * Returns the World at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The World at that Path
	 */ 
	public World getWorld(String path) {
		return Bukkit.getWorld(getString(path));
	}
	
	/**
	 * Returns the Double at the specified Path
	 *
	 * @param  path The path in the Config File
	 * @return      The Double at that Path
	 */
	public Double getDouble(String path) {
		return config.getDouble(path);
	}
	public Double getDouble(String path, double def) {
		return config.getDouble(path,def);
	}

	/**
	 * Returns all Paths in this Config
	 *
	 * @return      All Paths in this Config
	 */ 
	public Set<String> getKeys() {
		return config.getKeys(false);
	}
	
	/**
	 * Returns all Sub-Paths in this Config
	 *
	 * @param  path The path in the Config File
	 * @return      All Sub-Paths of the specified Path
	 */ 
	public Set<String> getKeys(String path) {
		if (contains(path)){
			return config.getConfigurationSection(path).getKeys(false);
		}
		return Collections.emptySet();
	}
	
	/**
	 * Reloads the Configuration File
	 */ 
	public void reload() {
		this.config = YamlConfiguration.loadConfiguration(this.theFile);
	}

	public ConfigurationSection getConfigurationSection(String path){
		return getConfiguration().getConfigurationSection(path);
	}
}
