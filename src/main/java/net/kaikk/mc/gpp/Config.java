package net.kaikk.mc.gpp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;

public class Config {
	// configuration variables, loaded/saved from a config.yml

	public boolean claims_preventTheft; // whether containers and crafting
	// blocks are protectable
	public boolean claims_protectCreatures; // whether claimed animals may be
	// injured by players without
	// permission
	public boolean claims_protectFires; // whether open flint+steel flames
										// should be protected - optional
	// because it's expensive
	public boolean claims_protectHorses; // whether horses on a claim should be
											// protected by that claim's rules
	public boolean claims_preventButtonsSwitches; // whether buttons and
	// switches are protectable
	public boolean claims_lockWoodenDoors; // whether wooden doors should be
											// locked by default (require
	// /accesstrust)
	public boolean claims_lockTrapDoors; // whether trap doors should be locked
	// by default (require /accesstrust)
	public boolean claims_lockFenceGates; // whether fence gates should be
	// locked by default (require
	// /accesstrust)
	public int claims_maxClaimsPerPlayer; // maximum number of claims per player
	public boolean claims_respectWorldGuard; // whether claim creations requires
	// WG build permission in
	// creation area
	public boolean claims_portalsRequirePermission; // whether nether portals

	// require permission to
	// generate. defaults to off
	// for performance reasons
	public int claims_initialBlocks; // the number of claim blocks a new player
										// starts with
	public double claims_abandonReturnRatio; // the portion of claim blocks
	// returned to a player when a
	// claim is abandoned
	public int claims_blocksAccruedPerHour; // how many additional blocks
											// players get each hour of play
	// (can be zero)
	public int claims_maxAccruedBlocks; // the limit on accrued blocks (over
										// time). doesn't limit purchased or
	// admin-gifted blocks
	public int claims_maxDepth; // limit on how deep claims can go
	public int claims_expirationDays; // how many days of inactivity before a

	// player loses his claims
	public int claims_automaticClaimsForNewPlayersRadius; // how big automatic
	// new player claims
	// (when they place
	// a chest) should
	// be. 0 to disable
	public int claims_minSize; // minimum width and height for non-admin claims

	public int claims_chestClaimExpirationDays; // number of days of inactivity
	// before an automatic chest
	// claim will be deleted

	public boolean claims_survivalAutoNatureRestoration; // whether survival

	// claims will be
	// automatically
	// restored to
	// nature when
	// auto-deleted
	public Material claims_investigationTool; // which material will be used to
	// investigate claims with a
	// right click
	public Material claims_modificationTool; // which material will be used to
	// create/resize claims with a
	// right click

	public ArrayList<String> claims_commandsRequiringAccessTrust; // the list of

	// messages (per player) in seconds
	public ArrayList<UUID> pvp_enabledWorlds; // list of worlds where pvp
												// anti-grief rules apply

	// combat
	public boolean pvp_noCombatInPlayerLandClaims; // whether players may fight
	// in player-owned land
	// claims
	public boolean pvp_noCombatInAdminLandClaims; // whether players may fight
	// in admin-owned land
	// claims
	public boolean pvp_noCombatInAdminSubdivisions; // whether players may fight
	// in subdivisions of
	// admin-owned land claims
	
	// death items are protected
	// in non-pvp worlds
	public double economy_claimBlocksPurchaseCost; // cost to purchase a claim
	// block. set to zero to
	// disable purchase.
	public double economy_claimBlocksSellValue; // return on a sold claim block.

	// set to zero to disable sale.
	public boolean blockClaimExplosions; // whether explosions may destroy
											// claimed blocks
	public boolean blockSurfaceCreeperExplosions; // whether creeper explosions
	// near or above the surface
	// destroy blocks
	public boolean blockSurfaceOtherExplosions; // whether non-creeper
	// explosions near or above the
	// surface destroy blocks
	public boolean blockSkyTrees; // whether players can build trees on

	// platforms in the sky
	public boolean fireSpreads; // whether fire spreads outside of claims
	public boolean fireDestroys; // whether fire destroys blocks outside of

	// by a banned player
	public boolean endermenMoveBlocks; // whether or not endermen may move
	// blocks around
	public boolean silverfishBreakBlocks; // whether silverfish may break blocks
	public boolean creaturesTrampleCrops; // whether or not non-player entities
	// may trample crops
	public boolean zombiesBreakDoors; // whether or not hard-mode zombies may

	// break down wooden doors
	public MaterialCollection mods_accessTrustIds; // list of block IDs which
	// should require
	// /accesstrust for player
	// interaction
	public MaterialCollection mods_containerTrustIds; // list of block IDs which
														// should require
	// /containertrust for
	// player interaction
	public List<String> mods_ignoreClaimsAccounts; // list of player names which
	// ALWAYS ignore claims
	public MaterialCollection mods_explodableIds; // list of block IDs which can
	// be destroyed by
	// explosions, even in
	// claimed areas

	public HashMap<String, Integer> seaLevelOverride; // override for sea level,

	// because bukkit
	// doesn't report the
	// right value for all
	// situations
	public boolean limitTreeGrowth; // whether trees should be prevented from
	// growing into a claim from outside
	public boolean pistonsInClaimsOnly; // whether pistons are limited to only
	
	// entrytrust default mode
	public boolean entryTrustAllowByDefault;

	// move blocks located within the
	// piston's land claim
	String databaseUrl;
	String databaseUserName;
	String databasePassword;
	boolean useLocalYMLInstead;

	public List<String> disabledWorlds;
	public List<String> claimRequiredWorlds;
	public List<String> creativeRulesWorlds;
	
	Config() {
		// load the config if it exists
		final FileConfiguration config = YamlConfiguration.loadConfiguration(new File(DataStore.configFilePath));
		final FileConfiguration outConfig = new YamlConfiguration();

		// read configuration settings (note defaults)

		final List<World> worlds = GriefPreventionPlus.getInstance().getServer().getWorlds();

		this.disabledWorlds = config.getStringList("GriefPrevention.Claims.DisabledWorlds");
		outConfig.set("GriefPrevention.Claims.DisabledWorlds", this.disabledWorlds);
		
		this.creativeRulesWorlds = config.getStringList("GriefPrevention.Claims.CreativeRulesWorlds");
		outConfig.set("GriefPrevention.Claims.CreativeRulesWorlds", this.creativeRulesWorlds);
		
		this.claimRequiredWorlds = config.getStringList("GriefPrevention.Claims.ClaimRequiredWorlds");
		outConfig.set("GriefPrevention.Claims.ClaimRequiredWorlds", this.claimRequiredWorlds);
		
		// pvp worlds list
		this.pvp_enabledWorlds = new ArrayList<UUID>();
		for (final World world : worlds) {
			final boolean pvpWorld = config.getBoolean("GriefPrevention.PvP.RulesEnabledInWorld." + world.getName(), world.getPVP());
			if (pvpWorld) {
				this.pvp_enabledWorlds.add(world.getUID());
			}
		}

		// sea level
		this.seaLevelOverride = new HashMap<String, Integer>();
		for (int i = 0; i < worlds.size(); i++) {
			final int seaLevelOverride = config.getInt("GriefPrevention.SeaLevelOverrides." + worlds.get(i).getName(), -1);
			outConfig.set("GriefPrevention.SeaLevelOverrides." + worlds.get(i).getName(), seaLevelOverride);
			this.seaLevelOverride.put(worlds.get(i).getName(), seaLevelOverride);
		}

		this.claims_preventTheft = config.getBoolean("GriefPrevention.Claims.PreventTheft", true);
		this.claims_protectCreatures = config.getBoolean("GriefPrevention.Claims.ProtectCreatures", true);
		this.claims_protectFires = config.getBoolean("GriefPrevention.Claims.ProtectFires", false);
		this.claims_protectHorses = config.getBoolean("GriefPrevention.Claims.ProtectHorses", true);
		this.claims_preventButtonsSwitches = config.getBoolean("GriefPrevention.Claims.PreventButtonsSwitches", true);
		this.claims_lockWoodenDoors = config.getBoolean("GriefPrevention.Claims.LockWoodenDoors", false);
		this.claims_lockTrapDoors = config.getBoolean("GriefPrevention.Claims.LockTrapDoors", false);
		this.claims_lockFenceGates = config.getBoolean("GriefPrevention.Claims.LockFenceGates", true);
		this.claims_initialBlocks = config.getInt("GriefPrevention.Claims.InitialBlocks", 100);
		this.claims_blocksAccruedPerHour = config.getInt("GriefPrevention.Claims.BlocksAccruedPerHour", 100);
		this.claims_maxAccruedBlocks = config.getInt("GriefPrevention.Claims.MaxAccruedBlocks", 80000);
		this.claims_abandonReturnRatio = config.getDouble("GriefPrevention.Claims.AbandonReturnRatio", 1);
		this.claims_automaticClaimsForNewPlayersRadius = config.getInt("GriefPrevention.Claims.AutomaticNewPlayerClaimsRadius", 4);
		this.claims_minSize = config.getInt("GriefPrevention.Claims.MinimumSize", 10);
		this.claims_maxDepth = config.getInt("GriefPrevention.Claims.MaximumDepth", 0);
		this.claims_chestClaimExpirationDays = config.getInt("GriefPrevention.Claims.Expiration.ChestClaimDays", 7);
		this.claims_expirationDays = config.getInt("GriefPrevention.Claims.Expiration.AllClaimDays", 0);
		this.claims_survivalAutoNatureRestoration = config.getBoolean("GriefPrevention.Claims.Expiration.AutomaticNatureRestoration.SurvivalWorlds", false);
		this.claims_maxClaimsPerPlayer = config.getInt("GriefPrevention.Claims.MaximumNumberOfClaimsPerPlayer", 0);
		this.claims_respectWorldGuard = config.getBoolean("GriefPrevention.Claims.CreationRequiresWorldGuardBuildPermission", true);
		this.claims_portalsRequirePermission = config.getBoolean("GriefPrevention.Claims.PortalGenerationRequiresPermission", false);
		final String accessTrustSlashCommands = config.getString("GriefPrevention.Claims.CommandsRequiringAccessTrust", "/sethome");

		this.economy_claimBlocksPurchaseCost = config.getDouble("GriefPrevention.Economy.ClaimBlocksPurchaseCost", 0);
		this.economy_claimBlocksSellValue = config.getDouble("GriefPrevention.Economy.ClaimBlocksSellValue", 0);

		this.blockClaimExplosions = config.getBoolean("GriefPrevention.BlockLandClaimExplosions", true);
		this.blockSurfaceCreeperExplosions = config.getBoolean("GriefPrevention.BlockSurfaceCreeperExplosions", true);
		this.blockSurfaceOtherExplosions = config.getBoolean("GriefPrevention.BlockSurfaceOtherExplosions", true);
		this.blockSkyTrees = config.getBoolean("GriefPrevention.LimitSkyTrees", true);
		this.limitTreeGrowth = config.getBoolean("GriefPrevention.LimitTreeGrowth", false);
		this.pistonsInClaimsOnly = config.getBoolean("GriefPrevention.LimitPistonsToLandClaims", true);

		this.fireSpreads = config.getBoolean("GriefPrevention.FireSpreads", false);
		this.fireDestroys = config.getBoolean("GriefPrevention.FireDestroys", false);

		this.endermenMoveBlocks = config.getBoolean("GriefPrevention.EndermenMoveBlocks", false);
		this.silverfishBreakBlocks = config.getBoolean("GriefPrevention.SilverfishBreakBlocks", false);
		this.creaturesTrampleCrops = config.getBoolean("GriefPrevention.CreaturesTrampleCrops", false);
		this.zombiesBreakDoors = config.getBoolean("GriefPrevention.HardModeZombiesBreakDoors", false);

		this.mods_ignoreClaimsAccounts = config.getStringList("GriefPrevention.Mods.PlayersIgnoringAllClaims");

		if (this.mods_ignoreClaimsAccounts == null) {
			this.mods_ignoreClaimsAccounts = new ArrayList<String>();
		}

		this.mods_accessTrustIds = new MaterialCollection();
		final List<String> accessTrustStrings = config.getStringList("GriefPrevention.Mods.BlockIdsRequiringAccessTrust");

		this.parseMaterialListFromConfig(accessTrustStrings, this.mods_accessTrustIds);

		this.mods_containerTrustIds = new MaterialCollection();
		final List<String> containerTrustStrings = config.getStringList("GriefPrevention.Mods.BlockIdsRequiringContainerTrust");

		// default values for container trust mod blocks
		if ((containerTrustStrings == null) || (containerTrustStrings.size() == 0)) {
			containerTrustStrings.add(new MaterialInfo(Material.CHEST, "Chest").toString());
		}

		// parse the strings from the config file
		this.parseMaterialListFromConfig(containerTrustStrings, this.mods_containerTrustIds);

		this.mods_explodableIds = new MaterialCollection();
		final List<String> explodableStrings = config.getStringList("GriefPrevention.Mods.BlockIdsExplodable");

		// parse the strings from the config file
		this.parseMaterialListFromConfig(explodableStrings, this.mods_explodableIds);

		// default for claim investigation tool
		String investigationToolMaterialName = Material.STICK.name();

		// get investigation tool from config
		investigationToolMaterialName = config.getString("GriefPrevention.Claims.InvestigationTool", investigationToolMaterialName);

		// validate investigation tool
		this.claims_investigationTool = Material.getMaterial(investigationToolMaterialName);
		if (this.claims_investigationTool == null) {
			GriefPreventionPlus.addLogEntry("ERROR: Material " + investigationToolMaterialName + " not found.  Defaulting to the stick.  Please update your config.yml.");
			this.claims_investigationTool = Material.STICK;
		}

		// default for claim creation/modification tool
		String modificationToolMaterialName = Material.GOLD_SPADE.name();

		// get modification tool from config
		modificationToolMaterialName = config.getString("GriefPrevention.Claims.ModificationTool", modificationToolMaterialName);

		// validate modification tool
		this.claims_modificationTool = Material.getMaterial(modificationToolMaterialName);
		if (this.claims_modificationTool == null) {
			GriefPreventionPlus.addLogEntry("ERROR: Material " + modificationToolMaterialName + " not found.  Defaulting to the golden shovel.  Please update your config.yml.");
			this.claims_modificationTool = Material.GOLD_SPADE;
		}

		this.pvp_noCombatInPlayerLandClaims = config.getBoolean("GriefPrevention.PvP.ProtectPlayersInLandClaims.PlayerOwnedClaims", true);
		this.pvp_noCombatInAdminLandClaims = config.getBoolean("GriefPrevention.PvP.ProtectPlayersInLandClaims.AdministrativeClaims", true);
		this.pvp_noCombatInAdminSubdivisions = config.getBoolean("GriefPrevention.PvP.ProtectPlayersInLandClaims.AdministrativeSubdivisions", true);

		// database settings
		this.databaseUrl = config.getString("GriefPrevention.Database.URL", "");
		this.databaseUserName = config.getString("GriefPrevention.Database.UserName", "");
		this.databasePassword = config.getString("GriefPrevention.Database.Password", "");
		this.useLocalYMLInstead = config.getBoolean("GriefPrevention.Database.useLocalYMLInstead", true);


		// entry trust
		this.entryTrustAllowByDefault = config.getBoolean("GriefPreventionPlus.EntryTrustAllowByDefault", true);

		outConfig.set("GriefPrevention.Claims.PreventTheft", this.claims_preventTheft);
		outConfig.set("GriefPrevention.Claims.ProtectCreatures", this.claims_protectCreatures);
		outConfig.set("GriefPrevention.Claims.PreventButtonsSwitches", this.claims_preventButtonsSwitches);
		outConfig.set("GriefPrevention.Claims.LockWoodenDoors", this.claims_lockWoodenDoors);
		outConfig.set("GriefPrevention.Claims.LockTrapDoors", this.claims_lockTrapDoors);
		outConfig.set("GriefPrevention.Claims.LockFenceGates", this.claims_lockFenceGates);
		outConfig.set("GriefPrevention.Claims.ProtectFires", this.claims_protectFires);
		outConfig.set("GriefPrevention.Claims.ProtectHorses", this.claims_protectHorses);
		outConfig.set("GriefPrevention.Claims.InitialBlocks", this.claims_initialBlocks);
		outConfig.set("GriefPrevention.Claims.BlocksAccruedPerHour", this.claims_blocksAccruedPerHour);
		outConfig.set("GriefPrevention.Claims.MaxAccruedBlocks", this.claims_maxAccruedBlocks);
		outConfig.set("GriefPrevention.Claims.AbandonReturnRatio", this.claims_abandonReturnRatio);
		outConfig.set("GriefPrevention.Claims.AutomaticNewPlayerClaimsRadius", this.claims_automaticClaimsForNewPlayersRadius);
		outConfig.set("GriefPrevention.Claims.MinimumSize", this.claims_minSize);
		outConfig.set("GriefPrevention.Claims.MaximumDepth", this.claims_maxDepth);
		outConfig.set("GriefPrevention.Claims.InvestigationTool", this.claims_investigationTool.name());
		outConfig.set("GriefPrevention.Claims.ModificationTool", this.claims_modificationTool.name());
		outConfig.set("GriefPrevention.Claims.Expiration.ChestClaimDays", this.claims_chestClaimExpirationDays);
		outConfig.set("GriefPrevention.Claims.Expiration.AllClaimDays", this.claims_expirationDays);
		outConfig.set("GriefPrevention.Claims.Expiration.AutomaticNatureRestoration.SurvivalWorlds", this.claims_survivalAutoNatureRestoration);
		outConfig.set("GriefPrevention.Claims.MaximumNumberOfClaimsPerPlayer", this.claims_maxClaimsPerPlayer);
		outConfig.set("GriefPrevention.Claims.CreationRequiresWorldGuardBuildPermission", this.claims_respectWorldGuard);
		outConfig.set("GriefPrevention.Claims.PortalGenerationRequiresPermission", this.claims_portalsRequirePermission);
		outConfig.set("GriefPrevention.Claims.CommandsRequiringAccessTrust", accessTrustSlashCommands);

		for (final World world : worlds) {
			outConfig.set("GriefPrevention.PvP.RulesEnabledInWorld." + world.getName(), this.pvp_enabledWorlds.contains(world.getUID()));
		}
		outConfig.set("GriefPrevention.PvP.ProtectPlayersInLandClaims.PlayerOwnedClaims", this.pvp_noCombatInPlayerLandClaims);
		outConfig.set("GriefPrevention.PvP.ProtectPlayersInLandClaims.AdministrativeClaims", this.pvp_noCombatInAdminLandClaims);
		outConfig.set("GriefPrevention.PvP.ProtectPlayersInLandClaims.AdministrativeSubdivisions", this.pvp_noCombatInAdminSubdivisions);

		outConfig.set("GriefPrevention.Economy.ClaimBlocksPurchaseCost", this.economy_claimBlocksPurchaseCost);
		outConfig.set("GriefPrevention.Economy.ClaimBlocksSellValue", this.economy_claimBlocksSellValue);

		outConfig.set("GriefPrevention.BlockLandClaimExplosions", this.blockClaimExplosions);
		outConfig.set("GriefPrevention.BlockSurfaceCreeperExplosions", this.blockSurfaceCreeperExplosions);
		outConfig.set("GriefPrevention.BlockSurfaceOtherExplosions", this.blockSurfaceOtherExplosions);
		outConfig.set("GriefPrevention.LimitSkyTrees", this.blockSkyTrees);
		outConfig.set("GriefPrevention.LimitTreeGrowth", this.limitTreeGrowth);
		outConfig.set("GriefPrevention.LimitPistonsToLandClaims", this.pistonsInClaimsOnly);

		outConfig.set("GriefPrevention.FireSpreads", this.fireSpreads);
		outConfig.set("GriefPrevention.FireDestroys", this.fireDestroys);

		outConfig.set("GriefPrevention.EndermenMoveBlocks", this.endermenMoveBlocks);
		outConfig.set("GriefPrevention.SilverfishBreakBlocks", this.silverfishBreakBlocks);
		outConfig.set("GriefPrevention.CreaturesTrampleCrops", this.creaturesTrampleCrops);
		outConfig.set("GriefPrevention.HardModeZombiesBreakDoors", this.zombiesBreakDoors);

		outConfig.set("GriefPrevention.Database.URL", this.databaseUrl);
		outConfig.set("GriefPrevention.Database.UserName", this.databaseUserName);
		outConfig.set("GriefPrevention.Database.Password", this.databasePassword);
		outConfig.set("GriefPrevention.Database.useLocalYMLInstead", this.useLocalYMLInstead);

		outConfig.set("GriefPrevention.Mods.BlockIdsRequiringAccessTrust", this.mods_accessTrustIds);
		outConfig.set("GriefPrevention.Mods.BlockIdsRequiringContainerTrust", this.mods_containerTrustIds);
		outConfig.set("GriefPrevention.Mods.BlockIdsExplodable", this.mods_explodableIds);
		outConfig.set("GriefPrevention.Mods.PlayersIgnoringAllClaims", this.mods_ignoreClaimsAccounts);
		outConfig.set("GriefPrevention.Mods.BlockIdsRequiringAccessTrust", accessTrustStrings);
		outConfig.set("GriefPrevention.Mods.BlockIdsRequiringContainerTrust", containerTrustStrings);
		outConfig.set("GriefPrevention.Mods.BlockIdsExplodable", explodableStrings);
		
		outConfig.set("GriefPreventionPlus.EntryTrustAllowByDefault", this.entryTrustAllowByDefault);

		try {
			outConfig.save(DataStore.configFilePath);
		} catch (final IOException exception) {
			GriefPreventionPlus.addLogEntry("Unable to write to the configuration file at \"" + DataStore.configFilePath + "\"");
		}

		// try to parse the list of commands requiring access trust in land
		// claims
		this.claims_commandsRequiringAccessTrust = new ArrayList<String>();
		String[] commands = accessTrustSlashCommands.split(";");
		for (int i = 0; i < commands.length; i++) {
			this.claims_commandsRequiringAccessTrust.add(commands[i].trim());
		}
	}

	private void parseMaterialListFromConfig(List<String> stringsToParse, MaterialCollection materialCollection) {
		materialCollection.clear();

		// for each string in the list
		for (int i = 0; i < stringsToParse.size(); i++) {
			// try to parse the string value into a material info
			final MaterialInfo materialInfo = MaterialInfo.fromString(stringsToParse.get(i));

			// null value returned indicates an error parsing the string from
			// the config file
			if (materialInfo == null) {
				// show error in log
				GriefPreventionPlus.addLogEntry("ERROR: Unable to read a material entry from the config file.  Please update your config.yml.");

				// update string, which will go out to config file to help user
				// find the error entry
				if (!stringsToParse.get(i).contains("can't")) {
					stringsToParse.set(i, stringsToParse.get(i) + "     <-- can't understand this entry, see BukkitDev documentation");
				}
			}

			// otherwise store the valid entry in config data
			else {
				materialCollection.add(materialInfo);
			}
		}
	}
}
