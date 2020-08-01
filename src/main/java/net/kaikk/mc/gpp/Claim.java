/*
    GriefPreventionPlus Server Plugin for Minecraft
    Copyright (C) 2015 Antonino Kai Pocorobba
    (forked from GriefPrevention by Ryan Hamshire)

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.kaikk.mc.gpp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.Chunk;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.World.Environment;
import org.bukkit.block.Block;
import org.bukkit.entity.Entity;
import org.bukkit.entity.Player;

//represents a player claim
//creating an instance doesn't make an effective claim
//only claims which have been added to the datastore have any effect
@SuppressWarnings("deprecation")
public class Claim {
	// id number. unique to this claim, never changes.
	Integer id;

	// Coordinates
	private UUID world;
	int lesserX, lesserZ, greaterX, greaterZ; // corners

	// ownerID. for admin claims, this is NULL
	// use getOwnerName() to get a friendly name (will be "an administrator" for
	// admin claims)
	private UUID ownerID;

	// modification date. this comes from the file timestamp during load, and is
	// updated with runtime changes
	private Date modifiedDate;

	// permissions for this claim
	private final HashMap<UUID, Integer> permissionMapPlayers = new HashMap<UUID, Integer>();
	private final HashMap<String, Integer> permissionMapBukkit = new HashMap<String, Integer>();
	private final HashMap<String, Integer> permissionMapFakePlayer = new HashMap<String, Integer>();

	private boolean areExplosivesAllowed = false;

	// parent claim
	// only used for claim subdivisions. top level claims have null here
	private Claim parent = null;

	// children (subdivisions)
	// note subdivisions themselves never have children
	private ArrayList<Claim> children = new ArrayList<Claim>();

	private final List<Material> placeableFarmingBlocksList = Arrays.asList(Material.PUMPKIN_STEM, Material.CROPS, Material.MELON_STEM, Material.CARROT, Material.POTATO, Material.NETHER_WARTS);

	long autoTrust;
	
	private long creationDate;

	Claim(Location lesserCorner, Location greaterCorner, UUID ownerID, HashMap<UUID, Integer> permissionMapPlayers, HashMap<String, Integer> permissionMapBukkit, HashMap<String, Integer> permissionMapFakePlayer, Integer id) {
		this(lesserCorner.getWorld().getUID(), lesserCorner.getBlockX(), lesserCorner.getBlockZ(), greaterCorner.getBlockX(), greaterCorner.getBlockZ(), ownerID, permissionMapPlayers, permissionMapBukkit, permissionMapFakePlayer, id);
	}

	Claim(UUID world, int lesserX, int lesserZ, int greaterX, int greaterZ, UUID ownerID, HashMap<UUID, Integer> permissionMapPlayers, HashMap<String, Integer> permissionMapBukkit, HashMap<String, Integer> permissionMapFakePlayer, Integer id) {
		this(world, lesserX, lesserZ, greaterX, greaterZ, ownerID, permissionMapPlayers, permissionMapBukkit, permissionMapFakePlayer, id, System.currentTimeMillis());
	}
	
	// main constructor. note that only creating a claim instance does nothing -
	// a claim must be added to the data store to be effective
	Claim(UUID world, int lesserX, int lesserZ, int greaterX, int greaterZ, UUID ownerID, HashMap<UUID, Integer> permissionMapPlayers, HashMap<String, Integer> permissionMapBukkit, HashMap<String, Integer> permissionMapFakePlayer, Integer id, long creationDate) {
		this.setModifiedDate(new Date());

		this.id = id;

		this.world = world;
		this.lesserX = lesserX;
		this.lesserZ = lesserZ;
		this.greaterX = greaterX;
		this.greaterZ = greaterZ;
		
		this.creationDate = creationDate;

		// owner
		this.setOwnerID(ownerID);
		if (permissionMapPlayers != null) {
			this.permissionMapPlayers.putAll(permissionMapPlayers);
		}
		if (permissionMapBukkit != null) {
			this.permissionMapBukkit.putAll(permissionMapBukkit);
		}

		if (permissionMapFakePlayer != null) {
			this.permissionMapFakePlayer.putAll(permissionMapFakePlayer);
		}
	}

	// whether more entities may be added to a claim
	public String allowMoreEntities() {
		if (this.getParent() != null) {
			return this.getParent().allowMoreEntities();
		}

		// this rule only applies to creative mode worlds
		if (!GriefPreventionPlus.getInstance().creativeRulesApply(this.getWorld())) {
			return null;
		}

		// admin claims aren't restricted
		if (this.isAdminClaim()) {
			return null;
		}

		// don't apply this rule to very large claims
		if (this.getArea() > 10000) {
			return null;
		}

		// determine maximum allowable entity count, based on claim size
		final int maxEntities = this.getArea() / 50;
		if (maxEntities == 0) {
			return GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.ClaimTooSmallForEntities);
		}

		// count current entities (ignoring players)
		int totalEntities = 0;
		final ArrayList<Chunk> chunks = this.getChunks();
		for (final Chunk chunk : chunks) {
			final Entity[] entities = chunk.getEntities();
			for (int i = 0; i < entities.length; i++) {
				final Entity entity = entities[i];
				if (!(entity instanceof Player) && this.contains(entity.getLocation(), false, false)) {
					totalEntities++;
					if (totalEntities > maxEntities) {
						entity.remove();
					}
				}
			}
		}

		if (totalEntities > maxEntities) {
			return GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.TooManyEntitiesInClaim);
		}

		return null;
	}

	public boolean areExplosivesAllowed() {
		return this.areExplosivesAllowed;
	}

	// entry permission check
	public String canEnter(Player player) {
		// admin claims need adminclaims permission only.
		if (this.isAdminClaim()) {
			if (player.hasPermission("griefprevention.adminclaims")) {
				return null;
			}
		}

		// players with this permission node can always enter the claim
		if (player.hasPermission("griefprevention.bypassentryprotection")) {
			return null;
		}

		// claim owner and admins in ignoreclaims mode have access
		if (player.getUniqueId().equals(this.getOwnerID()) || GriefPreventionPlus.getInstance().getDataStore().getPlayerData(player.getUniqueId()).ignoreClaims) {
			return null;
		}

		// if the entry trust mode is "allow by default", allow any player if there is no entry trust entry set
		if (GriefPreventionPlus.getInstance().config.entryTrustAllowByDefault && !this.hasExplicitEntryTrustEntry()) {
			return null;
		}

		// look for explicit (or public) individual access, inventory, or build
		// permission
		if (this.hasExplicitPermission(player, ClaimPermission.BUILD)) {
			return null;
		}
		if (this.hasExplicitPermission(player, ClaimPermission.ENTRY)) {
			return null;
		}
		if (this.hasExplicitPermission(player, ClaimPermission.ACCESS)) {
			return null;
		}
		if (this.hasExplicitPermission(player, ClaimPermission.CONTAINER)) {
			return null;
		}


		// also check for public permission
		if (this.hasPublicPermission(ClaimPermission.BUILD)) {
			return null;
		}
		if (this.hasPublicPermission(ClaimPermission.ENTRY)) {
			return null;
		}
		if (this.hasPublicPermission(ClaimPermission.ACCESS)) {
			return null;
		}
		if (this.hasPublicPermission(ClaimPermission.CONTAINER)) {
			return null;
		}


		// permission inheritance for subdivisions
		if (this.getParent() != null) {
			return this.getParent().canEnter(player);
		}

		// catch-all error message for all other cases
		String reason = GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.NoEntryPermission, this.getOwnerName());
		if (player.hasPermission("griefprevention.ignoreclaims")) {
			reason += "  " + GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.IgnoreClaimsAdvertisement);
		}
		return reason;
	}


	// access permission check
	public String canAccess(Player player) {
		// admin claims need adminclaims permission only.
		if (this.isAdminClaim()) {
			if (player.hasPermission("griefprevention.adminclaims")) {
				return null;
			}
		}

		// claim owner and admins in ignoreclaims mode have access
		if (player.getUniqueId().equals(this.getOwnerID()) || GriefPreventionPlus.getInstance().getDataStore().getPlayerData(player.getUniqueId()).ignoreClaims) {
			return null;
		}

		// look for explicit (or public) individual access, inventory, or build
		// permission
		if (this.hasExplicitPermission(player, ClaimPermission.ACCESS)) {
			return null;
		}
		if (this.hasExplicitPermission(player, ClaimPermission.CONTAINER)) {
			return null;
		}
		if (this.hasExplicitPermission(player, ClaimPermission.BUILD)) {
			return null;
		}

		// also check for public permission
		if (this.hasPublicPermission(ClaimPermission.ACCESS)) {
			return null;
		}
		if (this.hasPublicPermission(ClaimPermission.CONTAINER)) {
			return null;
		}
		if (this.hasPublicPermission(ClaimPermission.BUILD)) {
			return null;
		}

		// permission inheritance for subdivisions
		if (this.getParent() != null) {
			return this.getParent().canAccess(player);
		}

		// catch-all error message for all other cases
		String reason = GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.NoAccessPermission, this.getOwnerName());
		if (player.hasPermission("griefprevention.ignoreclaims")) {
			reason += "  " + GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.IgnoreClaimsAdvertisement);
		}
		return reason;
	}

	// break permission check
	public String canBreak(Player player, Material material) {
		// if not under siege, build rules apply
		return this.canBuild(player, material);
	}

	public String canBuild(Player player) {
		return this.canBuild(player, Material.AIR);
	}

	// build permission check
	public String canBuild(Player player, Material material) {
		// if we don't know who's asking, always say no (i've been told some
		// mods can make this happen somehow)
		if (player == null) {
			return "";
		}

		// admin claims can always be modified by admins, no exceptions
		if (this.isAdminClaim()) {
			if (player.hasPermission("griefprevention.adminclaims")) {
				return null;
			}
		}

		// owners can make changes, or admins with ignore claims mode enabled
		if (player.getUniqueId().equals(this.getOwnerID()) || GriefPreventionPlus.getInstance().getDataStore().getPlayerData(player.getUniqueId()).ignoreClaims) {
			return null;
		}

		// anyone with explicit build permission can make changes
		if (this.hasExplicitPermission(player, ClaimPermission.BUILD)) {
			return null;
		}

		// check for public permission
		if (this.hasPublicPermission(ClaimPermission.BUILD)) {
			return null;
		}

		// subdivision permission inheritance
		if (this.getParent() != null) {
			return this.getParent().canBuild(player, material);
		}

		// autotrust
		if (System.currentTimeMillis()<this.autoTrust) {
			this.autoTrust = 0;
			String trustMessage;
			if (Utils.isFakePlayer(player)) {
				this.setPermission("#"+player.getName(), ClaimPermission.BUILD);
				trustMessage = ChatColor.GREEN+"Fake player #"+player.getName()+" has been automatically trusted in your claim id "+this.getID();
			} else {
				this.setPermission(player.getUniqueId(), ClaimPermission.BUILD);
				trustMessage = ChatColor.GREEN+"Player "+ChatColor.RED+player.getName()+ChatColor.GREEN+" has been automatically trusted in your claim id "+this.getID();
			}

			Player owner = Bukkit.getPlayer(this.getOwnerID());
			if (owner!=null) {
				owner.sendMessage(trustMessage);
			}
			return null;
		}

		// failure message for all other cases
		String reason = GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.NoBuildPermission, this.getOwnerName());
		if (player.hasPermission("griefprevention.ignoreclaims")) {
			reason += "  " + GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.IgnoreClaimsAdvertisement);
		}

		// allow for farming with /containertrust permission
		if ((reason != null) && (this.canOpenContainers(player) == null)) {
			// do allow for farming, if player has /containertrust permission
			if (this.placeableForFarming(material)) {
				return null;
			}
		}

		return reason;
	}

	// permissions. note administrative "public" claims have different rules
	// than other claims
	// all of these return NULL when a player has permission, or a String error
	// message when the player doesn't have permission
	public String canEdit(Player player) {
		// if we don't know who's asking, always say no (i've been told some
		// mods can make this happen somehow)
		if (player == null) {
			return "";
		}

		// special cases...

		// admin claims need adminclaims permission only.
		if (this.isAdminClaim()) {
			if (player.hasPermission("griefprevention.adminclaims")) {
				return null;
			}
		}


		else {
			// the owner can also edit the claim
			if (player.getUniqueId().equals(this.ownerID)) {
				return null;
			}

			// anyone with deleteclaims permission can modify non-admin claims at
			// any time
			if (player.hasPermission("griefprevention.deleteclaims")) {
				return null;
			}
		}

		// permission inheritance for subdivisions
		if (this.getParent() != null) {
			return this.getParent().canEdit(player);
		}

		// error message if all else fails
		return GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.OnlyOwnersModifyClaims, this.getOwnerName());
	}

	// grant permission check, relatively simple
	public String canGrantPermission(Player player) // add permission level
	{
		// if we don't know who's asking, always say no (i've been told some
		// mods can make this happen somehow)
		if (player == null) {
			return "";
		}

		// anyone who can modify the claim can do this
		if (this.canEdit(player) == null) {
			return null;
		}

		if (this.hasExplicitPermission(player, ClaimPermission.MANAGE)) {
			return null;
		}

		// players ignoring claims can do this
		if (GriefPreventionPlus.getInstance().getDataStore().getPlayerData(player.getUniqueId()).ignoreClaims) {
			return null;
		}

		// permission inheritance for subdivisions
		if (this.getParent() != null) {
			return this.getParent().canGrantPermission(player);
		}

		// generic error message
		String reason = GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.NoPermissionTrust, this.getOwnerName());
		if (player.hasPermission("griefprevention.ignoreclaims")) {
			reason += "  " + GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.IgnoreClaimsAdvertisement);
		}
		return reason;
	}

	// inventory permission check
	public String canOpenContainers(Player player) {
		// if we don't know who's asking, always say no (i've been told some
		// mods can make this happen somehow)
		if (player == null) {
			return "";
		}

		// owner and administrators in ignoreclaims mode have access
		if (player.getUniqueId().equals(this.getOwnerID()) || GriefPreventionPlus.getInstance().getDataStore().getPlayerData(player.getUniqueId()).ignoreClaims) {
			return null;
		}

		// admin claims need adminclaims permission only.
		if (this.isAdminClaim()) {
			if (player.hasPermission("griefprevention.adminclaims")) {
				return null;
			}
		}

		// check for explicit individual container or build permission
		if (this.hasExplicitPermission(player, ClaimPermission.CONTAINER)) {
			return null;
		}
		if (this.hasExplicitPermission(player, ClaimPermission.BUILD)) {
			return null;
		}

		// check for public container or build permission
		if (this.hasPublicPermission(ClaimPermission.CONTAINER)) {
			return null;
		}
		if (this.hasPublicPermission(ClaimPermission.BUILD)) {
			return null;
		}

		// permission inheritance for subdivisions
		if (this.getParent() != null) {
			return this.getParent().canOpenContainers(player);
		}

		// error message for all other cases
		String reason = GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.NoContainersPermission, this.getOwnerName());
		if (player.hasPermission("griefprevention.ignoreclaims")) {
			reason += "  " + GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.IgnoreClaimsAdvertisement);
		}
		return reason;
	}

	public boolean checkHeight(int y) {
		return y >= GriefPreventionPlus.getInstance().config.claims_maxDepth;
	}

	public boolean checkHeight(Location loc) {
		return this.checkHeight(loc.getBlockY());
	}

	/**
	 * check if this player has that permission on this claim
	 *
	 * @return null string if the player has the permission
	 */
	public String checkPermission(Player player, Integer perm) {
		if (perm == null) {
			return "null";
		}
		if (this.canEdit(player) == null) { // owner and admins always pass this
			// check
			return null;
		}
		if ((perm & ClaimPermission.BUILD.perm) != 0) {
			return this.canBuild(player, Material.AIR);
		}
		if ((perm & ClaimPermission.CONTAINER.perm) != 0) {
			return this.canOpenContainers(player);
		}
		if ((perm & ClaimPermission.ACCESS.perm) != 0) {
			return this.canAccess(player);
		}
		if ((perm & ClaimPermission.ENTRY.perm) != 0) {
			return this.canEnter(player);
		}
		return "invalid";
	}

	/** clears all permissions (except owner of course) */
	public void clearPermissions() {
		this.clearMemoryPermissions();
		GriefPreventionPlus.getInstance().getDataStore().dbUnsetPerm(this.id);
	}

	/**
	 * whether or not a location is in a claim ignoreHeight = true means
	 * location UNDER the claim will return TRUE excludeSubdivisions = true
	 * means that locations inside subdivisions of the claim will return FALSE
	 */
	public boolean contains(Location location, boolean ignoreHeight, boolean excludeSubdivisions) {
		// not in the same world implies false
		if (!location.getWorld().getUID().equals(this.getWorldUID())) {
			return false;
		}

		final double x = location.getX();
		final double y = location.getY();
		final double z = location.getZ();

		// main check
		final boolean inClaim = (x >= this.lesserX) && (x < (this.greaterX + 1)) && (z >= this.lesserZ) && (z < (this.greaterZ + 1)) && (ignoreHeight || (y >= GriefPreventionPlus.getInstance().config.claims_maxDepth));

		if (!inClaim) {
			return false;
		}

		// additional check for subdivisions
		// you're only in a subdivision when you're also in its parent claim
		// NOTE: if a player creates subdivions then resizes the parent claim,
		// it's possible that
		// a subdivision can reach outside of its parent's boundaries. so this
		// check is important!
		if (this.getParent() != null) {
			return this.getParent().contains(location, ignoreHeight, false);
		}

		// code to exclude subdivisions in this check
		else if (excludeSubdivisions) {
			// search all subdivisions to see if the location is in any of them
			for (int i = 0; i < this.getChildren().size(); i++) {
				// if we find such a subdivision, return false
				if (this.getChildren().get(i).contains(location, ignoreHeight, true)) {
					return false;
				}
			}
		}

		// otherwise yes
		return true;
	}

	/** revokes a permission for a bukkit permission */
	public void dropPermission(String permBukkit) {
		this.unsetPermission(permBukkit);
		GriefPreventionPlus.getInstance().getDataStore().dbUnsetPerm(this.id, permBukkit);
	}

	/** revokes a permission for a player */
	public void dropPermission(UUID playerId) {
		this.unsetPermission(playerId);
		GriefPreventionPlus.getInstance().getDataStore().dbUnsetPerm(this.id, playerId);
	}

	// measurements. all measurements are in blocks
	public int getArea() {
		return this.getWidth() * this.getHeight();
	}

	public ArrayList<Claim> getChildren() {
		return this.children;
	}

	public ArrayList<Chunk> getChunks() {
		final ArrayList<Chunk> chunks = new ArrayList<Chunk>();

		final World world = this.getWorld();
		final Chunk lesserChunk = this.getLesserBoundaryCorner().getChunk();
		final Chunk greaterChunk = this.getGreaterBoundaryCorner().getChunk();

		for (int x = lesserChunk.getX(); x <= greaterChunk.getX(); x++) {
			for (int z = lesserChunk.getZ(); z <= greaterChunk.getZ(); z++) {
				chunks.add(world.getChunkAt(x, z));
			}
		}

		return chunks;
	}

	/**
	 * returns a copy of the location representing upper x, y, z limits NOTE:
	 * remember upper Y will always be ignored, all claims always extend to the
	 * sky
	 */
	public Location getGreaterBoundaryCorner() {
		return new Location(this.getWorld(), this.greaterX, 0, this.greaterZ);
	}

	public int getHeight() {
		return (this.greaterZ - this.lesserZ) + 1;
	}

	// accessor for ID
	public Integer getID() {
		return this.id;
	}

	/**
	 * returns a copy of the location representing lower x, y, z limits NOTE:
	 * remember upper Y will always be ignored, all claims always extend to the
	 * sky
	 */
	public Location getLesserBoundaryCorner() {
		return new Location(this.getWorld(), this.lesserX, 0, this.lesserZ);
	}

	public Date getModifiedDate() {
		return this.modifiedDate;
	}

	public UUID getOwnerID() {
		return this.parent==null ? this.ownerID : this.parent.ownerID;
	}

	/**
	 * returns a friendly owner name (for admin claims, returns
	 * "an administrator" as the owner)
	 */
	public String getOwnerName() {
		if (this.getParent() != null) {
			return this.getParent().getOwnerName();
		}

		if ((this.getOwnerID() == null) || this.getOwnerID().equals(GriefPreventionPlus.UUID1)) {
			return GriefPreventionPlus.getInstance().getDataStore().getMessage(Messages.OwnerNameForAdminClaims);
		}

		return GriefPreventionPlus.lookupPlayerName(this.getOwnerID());
	}

	public Claim getParent() {
		return this.parent;
	}

	// grants a permission for a bukkit permission
	public Integer getPermission(String target) {
		Integer perm = this.permissionMapBukkit.get(target);
		if (perm == null) {
			perm = this.permissionMapFakePlayer.get(target);
			if (perm == null) {
				perm = 0;
			}
		}
		return perm;
	}

	// grants a permission for a player or the public
	public Integer getPermission(UUID playerID) {
		Integer perm = this.permissionMapPlayers.get(playerID);
		if (perm == null) {
			perm = 0;
		}
		return perm;
	}

	public HashMap<String, Integer> getPermissionMapBukkit() {
		return this.permissionMapBukkit;
	}

	public HashMap<UUID, Integer> getPermissionMapPlayers() {
		return this.permissionMapPlayers;
	}

	public HashMap<String, Integer> getPermissionMapFakePlayers() {
		return this.permissionMapFakePlayer;
	}

	/**
	 * gets ALL permissions useful for listing all permissions in a claim
	 */
	public void getPermissions(ArrayList<String> builders, ArrayList<String> containers, ArrayList<String> accessors, ArrayList<String> enters, ArrayList<String> managers) {
		// loop through all the entries in the hash map
		for (final Entry<UUID, Integer> entry : this.permissionMapPlayers.entrySet()) {
			if (entry.getKey().equals(GriefPreventionPlus.UUID0)) {
				if ((entry.getValue() & ClaimPermission.MANAGE.perm) != 0) {
					managers.add("public");
				}

				if ((entry.getValue() & ClaimPermission.BUILD.perm) != 0) {
					builders.add("public");
				} else if ((entry.getValue() & ClaimPermission.CONTAINER.perm) != 0) {
					containers.add("public");
				} else if ((entry.getValue() & ClaimPermission.ACCESS.perm) != 0) {
					accessors.add("public");
				} else if ((entry.getValue() & ClaimPermission.ENTRY.perm) != 0) {
					enters.add("public");
				}
			} else {
				if ((entry.getValue() & ClaimPermission.MANAGE.perm) != 0) {
					managers.add(GriefPreventionPlus.getInstance().getServer().getOfflinePlayer(entry.getKey()).getName());
				}

				if ((entry.getValue() & ClaimPermission.BUILD.perm) != 0) {
					builders.add(GriefPreventionPlus.getInstance().getServer().getOfflinePlayer(entry.getKey()).getName());
				} else if ((entry.getValue() & ClaimPermission.CONTAINER.perm) != 0) {
					containers.add(GriefPreventionPlus.getInstance().getServer().getOfflinePlayer(entry.getKey()).getName());
				} else if ((entry.getValue() & ClaimPermission.ACCESS.perm) != 0) {
					accessors.add(GriefPreventionPlus.getInstance().getServer().getOfflinePlayer(entry.getKey()).getName());
				} else if ((entry.getValue() & ClaimPermission.ENTRY.perm) != 0) {
					enters.add(GriefPreventionPlus.getInstance().getServer().getOfflinePlayer(entry.getKey()).getName());
				}
			}
		}

		for (final Entry<String, Integer> entry : this.permissionMapBukkit.entrySet()) {
			if ((entry.getValue() & ClaimPermission.MANAGE.perm) != 0) {
				managers.add("[" + entry.getKey() + "]");
			}

			if ((entry.getValue() & ClaimPermission.BUILD.perm) != 0) {
				builders.add("[" + entry.getKey() + "]");
			} else if ((entry.getValue() & ClaimPermission.CONTAINER.perm) != 0) {
				containers.add("[" + entry.getKey() + "]");
			} else if ((entry.getValue() & ClaimPermission.ACCESS.perm) != 0) {
				accessors.add("[" + entry.getKey() + "]");
			} else if ((entry.getValue() & ClaimPermission.ENTRY.perm) != 0) {
				enters.add("[" + entry.getKey() + "]");
			}
		}

		for (final Entry<String, Integer> entry : this.permissionMapFakePlayer.entrySet()) {
			if ((entry.getValue() & ClaimPermission.MANAGE.perm) != 0) {
				managers.add("#" + entry.getKey());
			}

			if ((entry.getValue() & ClaimPermission.BUILD.perm) != 0) {
				builders.add("#" + entry.getKey());
			} else if ((entry.getValue() & ClaimPermission.CONTAINER.perm) != 0) {
				containers.add("#" + entry.getKey());
			} else if ((entry.getValue() & ClaimPermission.ACCESS.perm) != 0) {
				accessors.add("#" + entry.getKey());
			} else if ((entry.getValue() & ClaimPermission.ENTRY.perm) != 0) {
				enters.add("#" + entry.getKey());
			}
		}
	}

	public boolean hasExplicitEntryTrustEntry() {
		for (final Integer value : this.permissionMapPlayers.values()) {
			if ((value & ClaimPermission.ENTRY.perm) != 0) {
				return true;
			}
		}

		for (final Integer value : this.permissionMapBukkit.values()) {
			if ((value & ClaimPermission.ENTRY.perm) != 0) {
				return true;
			}
		}

		for (final Integer value : this.permissionMapFakePlayer.values()) {
			if ((value & ClaimPermission.ENTRY.perm) != 0) {
				return true;
			}
		}
		return false;
	}

	public String getTrustList() {
		final ArrayList<String> builders = new ArrayList<String>();
		final ArrayList<String> containers = new ArrayList<String>();
		final ArrayList<String> accessors = new ArrayList<String>();
		final ArrayList<String> enters = new ArrayList<String>();
		final ArrayList<String> managers = new ArrayList<String>();
		this.getPermissions(builders, containers, accessors, enters, managers);

		StringBuilder permissions = new StringBuilder("Explicit permissions here:");

		permissions.append("\n" + ChatColor.GOLD + "M: ");
		if (managers.size() > 0) {
			for (int i = 0; i < managers.size(); i++) {
				permissions.append(managers.get(i) + " ");
			}
		}

		permissions.append("\n" + ChatColor.YELLOW + "B: ");
		if (builders.size() > 0) {
			for (int i = 0; i < builders.size(); i++) {
				permissions.append(builders.get(i) + " ");
			}
		}

		permissions.append("\n" + ChatColor.GREEN + "C: ");
		if (containers.size() > 0) {
			for (int i = 0; i < containers.size(); i++) {
				permissions.append(containers.get(i) + " ");
			}
		}

		permissions.append("\n" + ChatColor.BLUE + "A: ");
		if (accessors.size() > 0) {
			for (int i = 0; i < accessors.size(); i++) {
				permissions.append(accessors.get(i) + " ");
			}
		}

		permissions.append("\n" + ChatColor.RED + "E: ");
		if (enters.size() > 0) {
			for (int i = 0; i < enters.size(); i++) {
				permissions.append(enters.get(i) + " ");
			}
		}

		permissions.append(ChatColor.RESET + "\n(" + ChatColor.GOLD + "M-anager" + ChatColor.RESET + ", " + ChatColor.YELLOW + "B-uilder" + ChatColor.RESET + ", " + ChatColor.GREEN + "C-ontainers" + ChatColor.RESET + ", " + ChatColor.BLUE + "A-ccess" + ChatColor.RESET + ", " + ChatColor.RED + "E-ntry)");

		return permissions.toString();
	}

	// gets the top claim ID
	public Integer getTopClaimID() {
		return this.getParent() != null ? this.getParent().getID() : this.getID();
	}

	public int getWidth() {
		return (this.greaterX - this.lesserX) + 1;
	}

	public World getWorld() {
		return GriefPreventionPlus.getInstance().getServer().getWorld(this.world);
	}

	public UUID getWorldUID() {
		return this.world;
	}

	// The player need that explicit permission
	public boolean hasExplicitPermission(Player player, ClaimPermission level) {
		if ((this.getPermission(player.getUniqueId()) & level.perm) != 0) {
			return true;
		}

		// check if the player has the default permissionBukkit permission
		switch (level) {
		case ACCESS:
			if (player.isPermissionSet("gpp.c" + this.id + ".a") && player.hasPermission("gpp.c" + this.id + ".a")) {
				return true;
			}
			break;
		case CONTAINER:
			if (player.isPermissionSet("gpp.c" + this.id + ".c") && player.hasPermission("gpp.c" + this.id + ".c")) {
				return true;
			}
			break;
		case BUILD:
			if (player.isPermissionSet("gpp.c" + this.id + ".b") && player.hasPermission("gpp.c" + this.id + ".b")) {
				return true;
			}
			break;
		case MANAGE:
			if (player.isPermissionSet("gpp.c" + this.id + ".m") && player.hasPermission("gpp.c" + this.id + ".m")) {
				return true;
			}
			break;
		case ENTRY:
			if (player.isPermissionSet("gpp.c" + this.id + ".e") && player.hasPermission("gpp.c" + this.id + ".e")) {
				return true;
			}
			break;
		}

		// check if the player has an explicit permissionBukkit permission
		for (final Entry<String, Integer> e : this.permissionMapBukkit.entrySet()) {
			if (((e.getValue() & level.perm) != 0) && player.isPermissionSet(e.getKey()) && player.hasPermission(e.getKey())) {
				return true;
			}
		}

		// check if player is a trusted fake player
		for (final Entry<String, Integer> e : this.permissionMapFakePlayer.entrySet()) {
			if (((e.getValue() & level.perm) != 0) && player.getName().startsWith(e.getKey())) {
				return true;
			}
		}

		return false;
	}

	public boolean hasPublicPermission(ClaimPermission level) {
		if ((this.getPermission(GriefPreventionPlus.UUID0) & level.perm) != 0) {
			return true;
		}

		return false;
	}

	// whether or not this is an administrative claim
	// administrative claims are created and maintained by players with the
	// griefprevention.adminclaims permission.
	public boolean isAdminClaim() {
		if (this.getParent() != null) {
			return this.getParent().isAdminClaim();
		}
		if (this.getOwnerID() == null) {
			return false;
		}
		return (this.getOwnerID().equals(GriefPreventionPlus.UUID1));
	}

	public boolean isInDataStore() {
		if (this.parent!=null) {
			if (GriefPreventionPlus.getInstance().getDataStore().getClaim(this.parent.getID()) != null) {
				return this.parent.children.contains(this);
			} else {
				return false;
			}
		}
		return GriefPreventionPlus.getInstance().getDataStore().getClaim(this.id) != null;
	}

	// distance check for claims, distance in this case is a band around the
	// outside of the claim rather then euclidean distance
	public boolean isNear(Location location, int howNear) {
		final Claim claim = new Claim(this.world, this.lesserX - howNear, this.lesserZ - howNear, this.greaterX + howNear, this.greaterZ + howNear, null, null, null, null, null);

		return claim.contains(location, false, true);
	}

	public String locationToString() {
		return "[" + (this.getWorld() != null ? this.getWorld().getName() : "(Invalid world "+this.getWorldUID()+")") + ", " + this.lesserX + "," + this.lesserZ + "~" + this.greaterX + "," + this.greaterZ + "]";
	}

	// removes any lava above sea level in a claim
	// exclusionClaim is another claim indicating an sub-area to be excluded
	// from this operation
	// it may be null
	public void removeSurfaceFluids(Claim exclusionClaim) {
		// don't do this for administrative claims
		if (this.isAdminClaim()) {
			return;
		}

		// don't do it for very large claims
		if (this.getArea() > 10000) {
			return;
		}
		
		// can't do this if the world is not loaded or doesn't exist
		if (this.getWorld() == null) {
			return;
		}

		// only in creative mode worlds
		if (!GriefPreventionPlus.getInstance().creativeRulesApply(this.getWorld())) {
			return;
		}

		final Location lesser = this.getLesserBoundaryCorner();
		final Location greater = this.getGreaterBoundaryCorner();

		if (lesser.getWorld().getEnvironment() == Environment.NETHER) {
			return; // don't clean up lava in the nether
		}

		int seaLevel = 0; // clean up all fluids in the end

		// respect sea level in normal worlds
		if (lesser.getWorld().getEnvironment() == Environment.NORMAL) {
			seaLevel = GriefPreventionPlus.getInstance().getSeaLevel(lesser.getWorld());
		}

		for (int x = lesser.getBlockX(); x <= greater.getBlockX(); x++) {
			for (int z = lesser.getBlockZ(); z <= greater.getBlockZ(); z++) {
				for (int y = seaLevel - 1; y <= lesser.getWorld().getMaxHeight(); y++) {
					// dodge the exclusion claim
					final Block block = lesser.getWorld().getBlockAt(x, y, z);
					if ((exclusionClaim != null) && exclusionClaim.contains(block.getLocation(), true, false)) {
						continue;
					}

					if ((block.getType() == Material.STATIONARY_LAVA) || (block.getType() == Material.LAVA)) {
						block.setType(Material.AIR);
					}
				}
			}
		}
	}

	public void setExplosivesAllowed(boolean areExplosivesAllowed) {
		this.areExplosivesAllowed = areExplosivesAllowed;
	}

	public void setLocation(UUID world, int lx, int lz, int gx, int gz) {
		this.world = world;
		this.lesserX = lx;
		this.lesserZ = lz;
		this.greaterX = gx;
		this.greaterZ = gz;
	}

	// grants a permission for a bukkit permission or fakeplayer
	public void setPermission(String target, ClaimPermission permissionLevel) {
		final Integer targetPermission = this.getPermission(target);
		if (target.startsWith("#")) {
			this.permissionMapFakePlayer.put(target.substring(1), targetPermission | permissionLevel.perm);
			GriefPreventionPlus.getInstance().getDataStore().dbSetPerm(this.id, target, permissionLevel.perm);
		} else if (target.startsWith("[") && target.endsWith("]")) {
			this.permissionMapBukkit.put(target.substring(1, target.length() - 1), targetPermission | permissionLevel.perm);
			GriefPreventionPlus.getInstance().getDataStore().dbSetPerm(this.id, target.substring(1, target.length() - 1), permissionLevel.perm);
		}
	}

	// grants a permission for a player or the public
	public void setPermission(UUID playerID, ClaimPermission permissionLevel) {
		final Integer currentPermission = this.getPermission(playerID);

		this.permissionMapPlayers.put(playerID, currentPermission | permissionLevel.perm);

		GriefPreventionPlus.getInstance().getDataStore().dbSetPerm(this.id, playerID, permissionLevel.perm);
	}

	private boolean placeableForFarming(Material material) {
		return this.placeableFarmingBlocksList.contains(material);
	}

	/**
	 * (this won't affect the database) clears all permissions (except owner of
	 * course)
	 */
	void clearMemoryPermissions() {
		this.permissionMapPlayers.clear();
		this.permissionMapBukkit.clear();
		this.permissionMapFakePlayer.clear();
	}

	long getPlayerInvestmentScore() {
		// decide which blocks will be considered player placed
		final ArrayList<Integer> playerBlocks = RestoreNatureProcessingTask.getPlayerBlocks(this.getWorld().getEnvironment(), this.getLesserBoundaryCorner().getBlock().getBiome());

		// scan the claim for player placed blocks
		double score = 0;

		final boolean creativeMode = GriefPreventionPlus.getInstance().creativeRulesApply(this.getWorld());

		for (int x = this.lesserX; x <= this.greaterX; x++) {
			for (int z = this.lesserZ; z <= this.greaterZ; z++) {
				int y = GriefPreventionPlus.getInstance().config.claims_maxDepth;
				for (; y < (GriefPreventionPlus.getInstance().getSeaLevel(this.getWorld()) - 5); y++) {
					final Block block = this.getWorld().getBlockAt(x, y, z);
					if (playerBlocks.contains(block.getTypeId())) {
						if ((block.getType() == Material.CHEST) && !creativeMode) {
							score += 10;
						} else if ((block.getType() != Material.DIRT) && (block.getType() != Material.STONE) && (block.getType() != Material.COBBLESTONE) && (block.getType() != Material.WOOD) && (block.getType() != Material.BEDROCK) && (block.getType() != Material.GRAVEL)) {
							score += .5;
						}
					}
				}

				for (; y < this.getWorld().getMaxHeight(); y++) {
					final Block block = this.getWorld().getBlockAt(x, y, z);
					if (playerBlocks.contains(block.getTypeId())) {
						if ((block.getType() == Material.CHEST) && !creativeMode) {
							score += 10;
						} else if (creativeMode && ((block.getType() == Material.LAVA) || (block.getType() == Material.STATIONARY_LAVA))) {
							score -= 10;
						} else {
							score += 1;
						}
					}
				}
			}
		}

		return (long) score;
	}

	// implements a strict ordering of claims, used to keep the claims
	// collection sorted for faster searching
	boolean greaterThan(Claim otherClaim) {
		final Location thisCorner = this.getLesserBoundaryCorner();
		final Location otherCorner = otherClaim.getLesserBoundaryCorner();

		if (thisCorner.getBlockX() > otherCorner.getBlockX()) {
			return true;
		}

		if (thisCorner.getBlockX() < otherCorner.getBlockX()) {
			return false;
		}

		if (thisCorner.getBlockZ() > otherCorner.getBlockZ()) {
			return true;
		}

		if (thisCorner.getBlockZ() < otherCorner.getBlockZ()) {
			return false;
		}

		return thisCorner.getWorld().getName().compareTo(otherCorner.getWorld().getName()) < 0;
	}

	// determines whether or not a claim has surface lava
	// used to warn players when they abandon their claims about automatic fluid
	// cleanup
	boolean hasSurfaceFluids() {
		final Location lesser = this.getLesserBoundaryCorner();
		final Location greater = this.getGreaterBoundaryCorner();

		// don't bother for very large claims, too expensive
		if (this.getArea() > 10000) {
			return false;
		}

		int seaLevel = 0; // clean up all fluids in the end

		// respect sea level in normal worlds
		if (lesser.getWorld().getEnvironment() == Environment.NORMAL) {
			seaLevel = GriefPreventionPlus.getInstance().getSeaLevel(lesser.getWorld());
		}

		for (int x = lesser.getBlockX(); x <= greater.getBlockX(); x++) {
			for (int z = lesser.getBlockZ(); z <= greater.getBlockZ(); z++) {
				for (int y = seaLevel - 1; y <= lesser.getWorld().getMaxHeight(); y++) {
					// dodge the exclusion claim
					final Block block = lesser.getWorld().getBlockAt(x, y, z);

					if ((block.getType() == Material.STATIONARY_LAVA) || (block.getType() == Material.LAVA)) {
						return true;
					}
				}
			}
		}

		return false;
	}

	// whether or not two claims overlap
	// used internally to prevent overlaps when creating claims
	boolean overlaps(Claim otherClaim) {
		// NOTE: if trying to understand this makes your head hurt, don't feel
		// bad - it hurts mine too.
		// try drawing pictures to visualize test cases.

		if (!this.getWorldUID().equals(otherClaim.getWorldUID())) {
			return false;
		}

		// first, check the corners of this claim aren't inside any existing
		// claims
		if (otherClaim.contains(this.getLesserBoundaryCorner(), true, false)) {
			return true;
		}
		if (otherClaim.contains(this.getGreaterBoundaryCorner(), true, false)) {
			return true;
		}
		if (otherClaim.contains(new Location(this.getWorld(), this.lesserX, 0, this.greaterZ), true, false)) {
			return true;
		}
		if (otherClaim.contains(new Location(this.getWorld(), this.greaterX, 0, this.lesserZ), true, false)) {
			return true;
		}

		// verify that no claim's lesser boundary point is inside this new
		// claim, to cover the "existing claim is entirely inside new claim"
		// case
		if (this.contains(otherClaim.getLesserBoundaryCorner(), true, false)) {
			return true;
		}

		// verify this claim doesn't band across an existing claim, either
		// horizontally or vertically
		if ((this.lesserZ <= otherClaim.greaterZ) && (this.lesserZ >= otherClaim.lesserZ) && (this.lesserX < otherClaim.lesserX) && (this.greaterX > otherClaim.greaterX)) {
			return true;
		}

		if ((this.greaterZ <= otherClaim.greaterZ) && (this.greaterZ >= otherClaim.lesserZ) && (this.lesserX < otherClaim.lesserX) && (this.greaterX > otherClaim.greaterX)) {
			return true;
		}

		if ((this.lesserX <= otherClaim.greaterX) && (this.lesserX >= otherClaim.lesserX) && (this.lesserZ < otherClaim.lesserZ) && (this.greaterZ > otherClaim.greaterZ)) {
			return true;
		}

		if ((this.greaterX <= otherClaim.greaterX) && (this.greaterX >= otherClaim.lesserX) && (this.lesserZ < otherClaim.lesserZ) && (this.greaterZ > otherClaim.greaterZ)) {
			return true;
		}

		return false;
	}

	void setChildren(ArrayList<Claim> children) {
		this.children = children;
	}

	void setModifiedDate(Date modifiedDate) {
		this.modifiedDate = modifiedDate;
	}

	void setOwnerID(UUID ownerID) {
		this.ownerID = ownerID;
	}

	void setParent(Claim parent) {
		this.parent = parent;
	}

	/**
	 * (this won't affect the database) revokes a permission for a bukkit
	 * permission or fakeplayer
	 */
	void unsetPermission(String target) {
		if (target.startsWith("#")) {
			this.permissionMapFakePlayer.remove(target.substring(1));
		} else {
			this.permissionMapBukkit.remove(target);
		}
	}

	/**
	 * (this won't affect the database) revokes a permission for a player or the
	 * public
	 */
	void unsetPermission(UUID playerID) {
		this.permissionMapPlayers.remove(playerID);
	}

	
	public boolean isSubdivision() {
		return this.parent!=null;
	}

	public long getCreationDate() {
		return creationDate;
	}
}
