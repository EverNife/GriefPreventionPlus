package net.kaikk.mc.gpp.events;

import net.kaikk.mc.gpp.Claim;
import org.bukkit.entity.Player;
import org.bukkit.event.Cancellable;
import org.bukkit.event.Event;
import org.bukkit.event.HandlerList;

import java.util.List;

/** Called when a user uses the command 'abandonallclaims' */
public class PreAbandonAllClaimsEvent extends Event implements Cancellable {

	private static final HandlerList handlerList = new HandlerList();
	private final List<Claim> claimList;
	private boolean isCancelled;
	private Player player;

	public PreAbandonAllClaimsEvent(List<Claim> claimList, Player player) {
		this.claimList = claimList;
		this.player = player;
	}

	public List<Claim> getClaimList() {
		return claimList;
	}

	public Player getPlayer() {
		return player;
	}

	@Override
	public boolean isCancelled() {
		return this.isCancelled;
	}

	@Override
	public void setCancelled(boolean cancel) {
		this.isCancelled = cancel;
	}

	@Override
	public HandlerList getHandlers() {
		return handlerList;
	}
}
